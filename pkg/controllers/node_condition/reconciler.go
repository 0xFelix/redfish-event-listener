/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package nodecondition

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redfishnode "github.com/0xfelix/redfish-event-listener/pkg/node"
)

func NewNodeConditionReconciler(apiClient client.Client) reconcile.Reconciler {
	return &nodeConditionReconciler{
		client: apiClient,
	}
}

type nodeConditionReconciler struct {
	client client.Client
}

// Reconcile main goal is to remove the condition set by the watchdog event
// A label holds the last transition time of the Ready condition, when the node is ready again,
// it waits until the Ready condition is True, and its LastTransitionTime is after the time stored in the label.
func (r *nodeConditionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	log := logf.FromContext(ctx).WithValues("NodeConditionController", req.Name)
	node := &corev1.Node{}

	if err := r.client.Get(ctx, req.NamespacedName, node); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Unable to fetch Node")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !shouldReconcile(node) {
		return ctrl.Result{}, nil
	}

	log.Info("Reconciling node watchdog conditions", "node", node.Name)

	patchHelper, err := patch.NewHelper(node, r.client)
	if err != nil {
		return ctrl.Result{}, err
	}

	defer func() {
		retErr = errors.Join(retErr, patchHelper.Patch(ctx, node))
		if retErr != nil {
			log.Error(retErr, "Failed to patch node")
		}
	}()

	lastTransitionTime, err := time.Parse(redfishnode.WatchdogResetTimeLabelFmt,
		node.Labels[redfishnode.WatchdogResetTimeLabel])
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to parse watchdog reset label time: %w", err)
	}

	readyCondition := findNodeCondition(node.Status.Conditions, corev1.NodeReady)
	if readyCondition.Status == corev1.ConditionTrue {
		if readyCondition.LastTransitionTime.After(lastTransitionTime) {
			log.Info("Ready condition lastTransitionTime is after watchdog reset label time, removing label and condition",
				"node", node.Name,
			)
			removeLabelAndCondition(node)
		} else {
			log.Info("Ready condition is true but it's lastTransitionTime is not after the watchdog reset label time"+
				"not removing label and condition",
				"node", node.Name,
			)
		}
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, nil
}

func shouldReconcile(node *corev1.Node) bool {
	// Look for the watchdog condition, this means a watchdog reset event has been received.
	watchdogConditionStatus := findNodeCondition(node.Status.Conditions, redfishnode.ConditionType)
	if watchdogConditionStatus == nil || watchdogConditionStatus.Status != corev1.ConditionFalse {
		return false
	}

	return findNodeCondition(node.Status.Conditions, corev1.NodeReady) != nil
}

func removeLabelAndCondition(node *corev1.Node) {
	delete(node.Labels, redfishnode.WatchdogResetTimeLabel)
	node.Status.Conditions = slices.DeleteFunc(node.Status.Conditions, func(c corev1.NodeCondition) bool {
		return c.Type == redfishnode.ConditionType
	})
}

func findNodeCondition(nodeConditions []corev1.NodeCondition, targetNodeConditionType corev1.NodeConditionType) *corev1.NodeCondition {
	for i := range nodeConditions {
		if nodeConditions[i].Type == targetNodeConditionType {
			return &nodeConditions[i]
		}
	}
	return nil
}
