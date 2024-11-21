// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"sort"

	"github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	"github.com/gardener/machine-controller-manager/pkg/controller/autoscaler"
	labelsutil "github.com/gardener/machine-controller-manager/pkg/util/labels"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/integer"
)

// rolloutAutoInPlace implements the logic for rolling  a machine set without replacing it.
func (dc *controller) rolloutAutoInPlace(ctx context.Context, d *v1alpha1.MachineDeployment, isList []*v1alpha1.MachineSet, machineMap map[types.UID]*v1alpha1.MachineList) error {
	clusterAutoscalerScaleDownAnnotations := make(map[string]string)
	clusterAutoscalerScaleDownAnnotations[autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationKey] = autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationValue

	// We do this to avoid accidentally deleting the user provided annotations.
	clusterAutoscalerScaleDownAnnotations[autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationByMCMKey] = autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationByMCMValue

	newIS, oldISs, err := dc.getAllMachineSetsAndSyncRevision(ctx, d, isList, machineMap, true)
	if err != nil {
		return err
	}
	allISs := append(oldISs, newIS)

	err = dc.taintNodesBackingMachineSets(
		ctx,
		oldISs, &v1.Taint{
			Key:    PreferNoScheduleKey,
			Value:  "True",
			Effect: "PreferNoSchedule",
		},
	)

	if dc.autoscalerScaleDownAnnotationDuringRollout {
		// Add the annotation on the all machinesets if there are any old-machinesets and not scaled-to-zero.
		// This also helps in annotating the node under new-machineset, incase the reconciliation is failing in next
		// status-rollout steps.
		if len(oldISs) > 0 && !dc.machineSetsScaledToZero(oldISs) {
			// Annotate all the nodes under this machine-deployment, as roll-out is on-going.
			err := dc.annotateNodesBackingMachineSets(ctx, allISs, clusterAutoscalerScaleDownAnnotations)
			if err != nil {
				klog.Errorf("Failed to add %s on all nodes. Error: %s", clusterAutoscalerScaleDownAnnotations, err)
				return err
			}
		}
	}

	if err != nil {
		klog.Warningf("Failed to add %s on all nodes. Error: %s", PreferNoScheduleKey, err)
	}

	// label all the machines and nodes backing the old machine sets as candidate for update
	err = dc.labelNodesBackingMachineSets(ctx, oldISs, map[string]string{v1alpha1.LabelKeyMachineCandidateForUpdate: "true"})
	if err != nil {
		return fmt.Errorf("failed to label nodes backing old machine sets as candidate for update: %v", err)
	}

	// In this section, we will attempt to scale up the new machine set. Machines with the `node.machine.sapcloud.io/update-successful` label
	// can transfer their ownership to the new machine set.
	// It is crucial to ensure that during the ownership transfer, the machine is not deleted,
	// and the old machine set is not scaled up to recreate the machine.
	scaledUp, err := dc.reconcileNewMachineSetInPlace(ctx, oldISs, newIS, d)
	if err != nil {
		klog.V(3).Infof("this was unexpected error")
		return err
	}
	if scaledUp {
		// Update DeploymentStatus
		return dc.syncRolloutStatus(ctx, allISs, newIS, d)
	}

	// prepare old ISs for update
	machinesSelectedForUpdate, err := dc.reconcileOldMachineSetsInPlace(ctx, allISs, FilterActiveMachineSets(oldISs), newIS, d)
	if err != nil {
		return err
	}
	if machinesSelectedForUpdate {
		// Update DeploymentStatus
		return dc.syncRolloutStatus(ctx, allISs, newIS, d)
	}

	if MachineDeploymentComplete(d, &d.Status) {
		if dc.autoscalerScaleDownAnnotationDuringRollout {
			// Check if any of the machine under this MachineDeployment contains the by-mcm annotation, and
			// remove the original autoscaler annotation only after.
			err := dc.removeAutoscalerAnnotationsIfRequired(ctx, allISs, clusterAutoscalerScaleDownAnnotations)
			if err != nil {
				return err
			}
		}
		if err := dc.cleanupMachineDeployment(ctx, oldISs, d); err != nil {
			return err
		}
	}

	// Sync deployment status
	return dc.syncRolloutStatus(ctx, allISs, newIS, d)
}

func (dc *controller) reconcileNewMachineSetInPlace(ctx context.Context, oldISs []*v1alpha1.MachineSet, newIS *v1alpha1.MachineSet, deployment *v1alpha1.MachineDeployment) (bool, error) {
	if (newIS.Spec.Replicas) == (deployment.Spec.Replicas) {
		// Scaling not required.
		return false, nil
	}
	if (newIS.Spec.Replicas) > (deployment.Spec.Replicas) {
		// Scale down.
		scaled, _, err := dc.scaleMachineSetAndRecordEvent(ctx, newIS, (deployment.Spec.Replicas), deployment)
		return scaled, err
	}

	klog.V(3).Infof("reconcile new machine set %s", newIS.Name)

	addedNewReplicasCount := int32(0)

	for _, is := range oldISs {
		transfferedMachineCount := int32(0)
		// get the machines for the machine set
		machines, err := dc.machineLister.List(labels.SelectorFromSet(is.Spec.Selector.MatchLabels))
		if err != nil {
			return false, err
		}

		klog.V(3).Infof("machine in old machine set %s: %d", is.Name, len(machines))

		// select the nodes which has been updated successfully
		nodes, err := dc.nodeLister.List(labels.SelectorFromSet(map[string]string{v1alpha1.LabelKeyMachineUpdateSuccessful: "true"}))
		if err != nil {
			return false, err
		}

		klog.V(3).Infof("nodes with update successful %v: %d", nodes, len(nodes))

		for _, node := range nodes {
			machine, err := getMachineFromNode(machines, node)
			// ignore error, if machine not found for the node.
			if err != nil {
				continue
			}

			klog.V(3).Infof("found machine for updated node, machine: %s, node: %s", machine.Name, node.Name)

			// removes labels not present in newIS so that the machine is not selected by the old machine set
			machineNewLabels := MergeStringMaps(MergeWithOverwriteAndFilter(machine.Labels, is.Spec.Selector.MatchLabels, newIS.Spec.Selector.MatchLabels), map[string]string{v1alpha1.LabelKeyMachineUpdateSuccessful: "true"})

			// update the owner reference of the machine to the new machine set and update the labels
			addControllerPatch := fmt.Sprintf(
				`{"metadata":{"ownerReferences":[{"apiVersion":"machine.sapcloud.io/v1alpha1","kind":"%s","name":"%s","uid":"%s","controller":true,"blockOwnerDeletion":true}],"labels":{%s},"uid":"%s"}}`,
				v1alpha1.SchemeGroupVersion.WithKind("MachineSet"),
				newIS.GetName(), newIS.GetUID(), labelsutil.GetFormatedLabels(machineNewLabels), machine.UID)

			err = dc.machineControl.PatchMachine(ctx, machine.Namespace, machine.Name, []byte(addControllerPatch))
			if err != nil {
				klog.V(3).Infof("failed to transfer the ownership of machine %s to new machine set %s", machine.Name, err)
				// scale up the new machine set to the already added replicas.
				scaled, _, err2 := dc.scaleMachineSetAndRecordEvent(ctx, newIS, newIS.Spec.Replicas+addedNewReplicasCount, deployment)
				klog.V(3).Infof("scale up after failure failed %s", err2)
				if err2 != nil {
					return addedNewReplicasCount > 0, err2
				}

				klog.V(3).Infof("scale down machine set %s, transffered replicas to new machine set %d", is.Name, transfferedMachineCount)
				_, _, err2 = dc.scaleMachineSetAndRecordEvent(ctx, is, is.Spec.Replicas-transfferedMachineCount, deployment)
				klog.V(3).Infof("scale down after failure failed %s", err2)
				if err2 != nil {
					klog.V(3).Infof("scale down failed %s", err)
					return addedNewReplicasCount > 0, err
				}

				return scaled, err
			}

			// uncordon the node since the ownership of the machine has been transferred to the new machien set.
			node.Spec.Unschedulable = false
			_, err = dc.controlCoreClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to uncordon the node %s", node.Name)
			}

			transfferedMachineCount++ // scale down the old machine set.
			addedNewReplicasCount++   // scale up the new machine set.
		}

		klog.V(3).Infof("scale down machine set %s, transffered replicas to new machine set %d", is.Name, transfferedMachineCount)
		_, _, err = dc.scaleMachineSetAndRecordEvent(ctx, is, is.Spec.Replicas-transfferedMachineCount, deployment)
		if err != nil {
			klog.V(3).Infof("scale down failed %s", err)
			return addedNewReplicasCount > 0, err
		}
	}

	klog.V(3).Infof("scale up the new machine set %s by %d", newIS.Name, addedNewReplicasCount)
	scaled, _, err := dc.scaleMachineSetAndRecordEvent(ctx, newIS, newIS.Spec.Replicas+addedNewReplicasCount, deployment)
	return scaled, err
}

func (dc *controller) reconcileOldMachineSetsInPlace(ctx context.Context, allISs []*v1alpha1.MachineSet, oldISs []*v1alpha1.MachineSet, newIS *v1alpha1.MachineSet, deployment *v1alpha1.MachineDeployment) (bool, error) {
	oldMachinesCount := GetReplicaCountForMachineSets(oldISs)
	if oldMachinesCount == 0 {
		// Can't scale down further
		return false, nil
	}

	allMachinesCount := GetReplicaCountForMachineSets(allISs)
	klog.V(3).Infof("New machine set %s has %d available machines.", newIS.Name, newIS.Status.AvailableReplicas)
	maxUnavailable := MaxUnavailable(*deployment)

	minAvailable := (deployment.Spec.Replicas) - maxUnavailable
	newISUnavailableMachineCount := (newIS.Spec.Replicas) - newIS.Status.AvailableReplicas
	oldISsMachinesUndergoingUpdate, err := dc.getMachinesUndergoingUpdate(oldISs)
	if err != nil {
		return false, err
	}

	klog.V(3).Infof("allMachinesCount:%d,  minAvailable:%d,  newISUnavailableMachineCount:%d,  oldISsMachineInUpdateProcess:%d", allMachinesCount, minAvailable, newISUnavailableMachineCount, oldISsMachinesUndergoingUpdate)

	maxUpdatePossible := allMachinesCount - minAvailable - newISUnavailableMachineCount - oldISsMachinesUndergoingUpdate
	if maxUpdatePossible <= 0 {
		return false, nil
	}

	// preapre machines from old machine sets to get updated, need to check maxUnavailable to ensure we can select machines for update.
	machinesSelectedForUpdate, err := dc.selectMachineForUpdate(ctx, allISs, oldISs, deployment, oldISsMachinesUndergoingUpdate)
	if err != nil {
		return false, err
	}

	return machinesSelectedForUpdate > 0, nil
}

func (dc *controller) selectMachineForUpdate(ctx context.Context, allISs []*v1alpha1.MachineSet, oldISs []*v1alpha1.MachineSet, deployment *v1alpha1.MachineDeployment, oldISsMachinesUndergoingUpdate int32) (int32, error) {
	maxUnavailable := MaxUnavailable(*deployment)

	// Check if we can pick machines from old ISes for updating to new IS.
	minAvailable := (deployment.Spec.Replicas) - maxUnavailable

	// Find the number of available machines.
	availableMachineCount := GetAvailableReplicaCountForMachineSets(allISs) - oldISsMachinesUndergoingUpdate
	if availableMachineCount <= minAvailable {
		// Cannot pick for updating.
		return 0, nil
	}

	sort.Sort(MachineSetsByCreationTimestamp(oldISs))

	totalReadyForUpdate := int32(0)
	totalReadyForUpdateCount := availableMachineCount - minAvailable
	for _, targetIS := range oldISs {
		if totalReadyForUpdate >= totalReadyForUpdateCount {
			// No further updating required.
			break
		}
		if (targetIS.Spec.Replicas) == 0 {
			// cannot pick this ReplicaSet.
			continue
		}
		// prepare for update
		readyForUpdateCount := int32(integer.IntMin(int((targetIS.Spec.Replicas)), int(totalReadyForUpdateCount-totalReadyForUpdate)))
		newReplicasCount := (targetIS.Spec.Replicas) - readyForUpdateCount

		if newReplicasCount > (targetIS.Spec.Replicas) {
			return 0, fmt.Errorf("when selecting machine from old IS for update, got invalid request %s %d -> %d", targetIS.Name, (targetIS.Spec.Replicas), newReplicasCount)
		}
		machinesSelectedForUpdate, err := dc.labelMachinesToSelectedForUpdate(ctx, targetIS, newReplicasCount)
		if err != nil {
			return totalReadyForUpdate + machinesSelectedForUpdate, err
		}

		totalReadyForUpdate += machinesSelectedForUpdate
	}

	return totalReadyForUpdate, nil
}

// labelNodesBackingMachineSets annotates all nodes backing the machineSets
func (dc *controller) labelNodesBackingMachineSets(ctx context.Context, machineSets []*v1alpha1.MachineSet, label map[string]string) error {
	for _, machineSet := range machineSets {

		if machineSet == nil {
			continue
		}

		klog.V(4).Infof("Trying to label nodes under the MachineSet object %q with %v", machineSet.Name, label)
		filteredMachines, err := dc.machineLister.List(labels.SelectorFromSet(machineSet.Spec.Selector.MatchLabels))
		if err != nil {
			return err
		}

		for _, machine := range filteredMachines {
			if machine.Labels[v1alpha1.NodeLabelKey] != "" {
				labels := MergeStringMaps(machine.Labels, map[string]string{v1alpha1.LabelKeyMachineCandidateForUpdate: "true"})
				addLabelPatch := fmt.Sprintf(`{"metadata":{"labels":{%s}}}`, labelsutil.GetFormatedLabels(labels))

				klog.V(3).Infof("adding label to machine %s candidate for update %s", machine.Name, labels)
				if err := dc.machineControl.PatchMachine(ctx, machine.Namespace, machine.Name, []byte(addLabelPatch)); err != nil {
					klog.V(3).Infof("error while adding label candidate for update %s", err)
					return err
				}

				node, err := dc.nodeLister.Get(machine.Labels[v1alpha1.NodeLabelKey])
				if err != nil {
					if apierrors.IsNotFound(err) {
						continue // Node is not found, continue to the next machine
					}
					klog.Errorf("Error occurred while trying to fetch node object - err: %s", err)
					return err
				}
				if node.Labels[v1alpha1.LabelKeyMachineCandidateForUpdate] == "true" {
					continue
				}
				nodeCopy := node.DeepCopy()
				if nodeCopy.Labels == nil {
					nodeCopy.Labels = make(map[string]string)
				}
				nodeCopy.Labels[v1alpha1.LabelKeyMachineCandidateForUpdate] = "true"
				if _, err := dc.targetCoreClient.CoreV1().Nodes().Update(ctx, nodeCopy, metav1.UpdateOptions{}); err != nil {
					return err
				}
			}
		}
		klog.V(3).Infof("Labeled the nodes backed by MachineSet %q with %v", machineSet.Name, label)
	}

	return nil
}

func (dc *controller) labelMachinesToSelectedForUpdate(ctx context.Context, is *v1alpha1.MachineSet, newScale int32) (int32, error) {
	machinesSelectedForUpdate := int32(0)
	readyForDrain := is.Spec.Replicas - newScale

	machines, err := dc.getMachinesForDrain(is, readyForDrain)
	if err != nil {
		return machinesSelectedForUpdate, err
	}

	klog.V(3).Infof("machines selected for drain %v", machines)

	for _, machine := range machines {
		labels := MergeStringMaps(machine.Labels, map[string]string{v1alpha1.LabelKeyMachineSelectedForUpdate: "true"})
		addLabelPatch := fmt.Sprintf(`{"metadata":{"labels":{%s}}}`, labelsutil.GetFormatedLabels(labels))

		// based on this label, the machine-controller will cordon and drain the machine. MCM provieders will do this work.
		klog.V(3).Infof("adding label to machine %s selected-for-update %s", machine.Name, labels)
		if err := dc.machineControl.PatchMachine(ctx, machine.Namespace, machine.Name, []byte(addLabelPatch)); err != nil {
			klog.V(3).Infof("error while adding label selected-for-update %s", err)
			return machinesSelectedForUpdate, err
		}

		machinesSelectedForUpdate++
	}

	return machinesSelectedForUpdate, nil
}

func (dc *controller) getMachinesUndergoingUpdate(oldISs []*v1alpha1.MachineSet) (int32, error) {
	machineInUpdateProcess := int32(0)
	for _, is := range oldISs {
		machines, err := dc.machineLister.List(labels.SelectorFromSet(MergeStringMaps(is.Spec.Selector.MatchLabels, map[string]string{v1alpha1.LabelKeyMachineSelectedForUpdate: "true"})))
		if err != nil {
			return 0, err
		}
		machineInUpdateProcess += int32(len(machines))
	}

	return machineInUpdateProcess, nil
}

func (dc *controller) getMachinesForDrain(is *v1alpha1.MachineSet, readyForDrain int32) ([]*v1alpha1.Machine, error) {
	// select only those machines which are candidate for update and not selected for update.
	machines, err := dc.machineLister.List(labels.SelectorFromSet(MergeStringMaps(is.Spec.Selector.MatchLabels, map[string]string{v1alpha1.LabelKeyMachineCandidateForUpdate: "true"})))
	if err != nil {
		return nil, err
	}

	// delete machines which are already selected for update.
	machinesWithoutSelectedForUpdate := []*v1alpha1.Machine{}
	for i := 0; i < len(machines); i++ {
		if _, ok := machines[i].Labels[v1alpha1.LabelKeyMachineSelectedForUpdate]; !ok {
			machinesWithoutSelectedForUpdate = append(machinesWithoutSelectedForUpdate, machines[i])
		}
	}

	// Get readyForDrain count of machines from the machine set randomly.
	if len(machinesWithoutSelectedForUpdate) > int(readyForDrain) {
		return machinesWithoutSelectedForUpdate[:readyForDrain], nil
	}
	return machinesWithoutSelectedForUpdate, nil
}
