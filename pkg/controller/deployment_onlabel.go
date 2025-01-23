// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"

	"github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	"github.com/gardener/machine-controller-manager/pkg/controller/autoscaler"
	labelsutil "github.com/gardener/machine-controller-manager/pkg/util/labels"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// onLabelInPlace implements the logic for rolling  a machine set without replacing it.
func (dc *controller) rolloutManualInPlace(ctx context.Context, d *v1alpha1.MachineDeployment, isList []*v1alpha1.MachineSet, machineMap map[types.UID]*v1alpha1.MachineList) error {
	clusterAutoscalerScaleDownAnnotations := make(map[string]string)
	clusterAutoscalerScaleDownAnnotations[autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationKey] = autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationValue

	// We do this to avoid accidentally deleting the user provided annotations.
	clusterAutoscalerScaleDownAnnotations[autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationByMCMKey] = autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationByMCMValue

	newIS, oldISs, err := dc.getAllMachineSetsAndSyncRevision(ctx, d, isList, machineMap, true)
	if err != nil {
		return err
	}
	allISs := append(oldISs, newIS)

	// TODO: Do we need to do it for all the nodes or only for the nodes which are undergoing update?
	err = dc.taintNodesBackingMachineSets(
		ctx,
		oldISs, &v1.Taint{
			Key:    PreferNoScheduleKey,
			Value:  "True",
			Effect: "PreferNoSchedule",
		},
	)

	if len(oldISs) > 0 && !dc.machineSetsScaledToZero(oldISs) {
		// Label all the old machine sets to skip the scale up.
		err := dc.labelMachineSets(ctx, oldISs, map[string]string{v1alpha1.LabelKeyMachineSetSkipUpdate: "true"})
		if err != nil {
			klog.Errorf("Failed to add %s on all machine sets. Error: %s", v1alpha1.LabelKeyMachineSetSkipUpdate, err)
			return err
		}
	}

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

	if err := dc.syncMachineSets(ctx, oldISs, newIS, d); err != nil {
		fmt.Printf("failed to sync machine sets %w", err)
		return fmt.Errorf("failed to sync machine sets %s", err)
	}

	// In this section, we will attempt to scale up the new machine set. Machines with the `machine.sapcloud.io/update-successful` label
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

	// Get the nodes belonging to the machine sets with the selected for update label and mark the machine with the label.
	// Label machines with the selected for update label.
	machinesSelectedForUpdate, err := dc.getAndLabelMachinesSelectedForUpdate(ctx, oldISs)
	if err != nil {
		klog.Errorf("failed to get and label machines to selected for update %s", err)
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

// Get and patch the machines whose nodes were marked for update.
func (dc *controller) getAndLabelMachinesSelectedForUpdate(ctx context.Context, iSs []*v1alpha1.MachineSet) (bool, error) {
	isMachineSelectedForUpdate := false

	for _, is := range iSs {
		if (is.Spec.Replicas) == 0 {
			// cannot pick this ReplicaSet.
			continue
		}

		machines, err := dc.machineLister.List(labels.SelectorFromSet(is.Spec.Selector.MatchLabels))
		if err != nil {
			return false, err
		}

		for _, machine := range machines {
			if machine.Labels[v1alpha1.NodeLabelKey] != "" {
				node, err := dc.targetCoreClient.CoreV1().Nodes().Get(ctx, machine.Labels[v1alpha1.NodeLabelKey], metav1.GetOptions{})
				if err != nil {
					klog.Warningf("Cannot get node: %s, Error: %s", machine.Labels[v1alpha1.NodeLabelKey], err)
					continue
				}

				// If the node is not marked for update, do not process the node.
				if _, ok := node.Labels[v1alpha1.LabelKeyMachineSelectedForUpdate]; !ok {
					continue
				}

				// Reached Here: Means node is marked for update.
				// Node is marked for update. Label the corresponding machine with selected-for-update label.
				labels := MergeStringMaps(machine.Labels, map[string]string{v1alpha1.LabelKeyMachineSelectedForUpdate: "true"})
				addLabelPatch := fmt.Sprintf(`{"metadata":{"labels":{%s}}}`, labelsutil.GetFormatedLabels(labels))

				// based on this label, the machine-controller will cordon and drain the machine. MCM provieders will do this work.
				klog.V(3).Infof("adding label to machine %s selected-for-update %s", machine.Name, labels)
				if err := dc.machineControl.PatchMachine(ctx, machine.Namespace, machine.Name, []byte(addLabelPatch)); err != nil {
					klog.V(3).Infof("error while adding label selected-for-update %s", err)
					return false, err
				}

				// machine is marked for update.
				isMachineSelectedForUpdate = true
			}
		}

	}

	return isMachineSelectedForUpdate, nil
}
