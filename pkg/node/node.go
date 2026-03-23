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

package node

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ConditionType          = "RedfishWatchdogEvent"
	WatchdogResetTimeLabel = "redfish.event.listener/last-watchdog-reset-time"
	// WatchdogResetTimeLabelFmt is a label-safe RFC3339-like format (hyphens instead of colons in the time part).
	// Example: 2026-02-17T12-11-43Z
	WatchdogResetTimeLabelFmt = "2006-01-02T15-04-05Z"
)

func UpdateNodeCondition(k8sClient kubernetes.Interface, nodeName string) error {
	node, err := k8sClient.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node: %w", err)
	}

	now := metav1.Now()
	newCondition := corev1.NodeCondition{
		Type:               ConditionType,
		Status:             corev1.ConditionTrue,
		LastHeartbeatTime:  now,
		LastTransitionTime: now,
		Reason:             "EventReceived",
		Message:            "Redfish watchdog expired event received",
	}

	conditionExists := false
	for i, condition := range node.Status.Conditions {
		if condition.Type == ConditionType {
			node.Status.Conditions[i] = newCondition
			conditionExists = true
			break
		}
	}
	if !conditionExists {
		node.Status.Conditions = append(node.Status.Conditions, newCondition)
	}

	_, err = k8sClient.CoreV1().Nodes().UpdateStatus(context.Background(), node, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update node status: %w", err)
	}

	patch := client.MergeFrom(node.DeepCopy())
	// Use label-safe time format (no colons); Kubernetes labels only allow [A-Za-z0-9_.-]
	node.Labels[WatchdogResetTimeLabel] = now.Format(WatchdogResetTimeLabelFmt)

	patchBytes, err := patch.Data(node)
	if err != nil {
		return fmt.Errorf("failed to patch data: %w", err)
	}

	_, err = k8sClient.CoreV1().Nodes().Patch(context.Background(), nodeName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch node labels: %w", err)
	}

	return nil
}
