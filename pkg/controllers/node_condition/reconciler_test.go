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

package nodecondition_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nodecondition "github.com/0xfelix/redfish-event-listener/pkg/controllers/node_condition"
	"github.com/0xfelix/redfish-event-listener/pkg/node"
)

var _ = Describe("NodeConditionReconciler", func() {
	const nodeName = "node1"

	var (
		fakeClient client.Client
		reconciler reconcile.Reconciler
		ctx        context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeClient = fake.NewFakeClient()
		reconciler = nodecondition.NewNodeConditionReconciler(fakeClient)
	})

	DescribeTable("should not do anything when",
		func(conds []corev1.NodeCondition) {
			n := newNode(nodeName, conds, map[string]string{node.WatchdogResetTimeLabel: "2025-07-04T18-30-00Z"})
			Expect(fakeClient.Create(ctx, n)).To(Succeed())

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: nodeName}})
			Expect(err).NotTo(HaveOccurred())

			Expect(fakeClient.Get(ctx, types.NamespacedName{Name: nodeName}, n)).To(Succeed())
			Expect(n.Labels).To(HaveKeyWithValue(node.WatchdogResetTimeLabel, "2025-07-04T18-30-00Z"))
		},
		Entry("the watchdog condition is missing", []corev1.NodeCondition{*nodeReadyTrueCondition()}),
		Entry("the Ready condition is missing", []corev1.NodeCondition{*watchdogCondition()}),
	)

	DescribeTable("should do not remove the label and condition if the node",
		func(conds []corev1.NodeCondition, labelValue string) {
			n := newNode(nodeName, conds, map[string]string{node.WatchdogResetTimeLabel: labelValue})
			Expect(fakeClient.Create(ctx, n)).To(Succeed())

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: nodeName}})
			Expect(err).NotTo(HaveOccurred())

			Expect(fakeClient.Get(ctx, types.NamespacedName{Name: nodeName}, n)).To(Succeed())
			Expect(n.Labels).To(HaveKeyWithValue(node.WatchdogResetTimeLabel, labelValue))
			Expect(n.Status.Conditions).To(ContainElement(*watchdogCondition()))
		},
		Entry("is not ready",
			[]corev1.NodeCondition{*watchdogCondition(), *nodeReadyFalseCondition()},
			"2024-07-04T18-30-00Z",
		),
		Entry("is ready but the last transition time is before the label time",
			[]corev1.NodeCondition{*watchdogCondition(), *nodeReadyTrueCondition()},
			"2026-07-04T18-30-00Z",
		),
	)

	It("should remove the label and condition if the node is ready", func() {
		n := newNode(nodeName,
			[]corev1.NodeCondition{*watchdogCondition(), *nodeReadyTrueCondition()},
			map[string]string{node.WatchdogResetTimeLabel: "2025-07-04T18-30-00Z"},
		)
		Expect(fakeClient.Create(ctx, n)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: nodeName}})
		Expect(err).NotTo(HaveOccurred())

		Expect(fakeClient.Get(ctx, types.NamespacedName{Name: nodeName}, n)).To(Succeed())
		Expect(n.Labels).NotTo(HaveKey(node.WatchdogResetTimeLabel))
		Expect(n.Status.Conditions).NotTo(ContainElement(*watchdogCondition()))
	})
})

func newNode(nodeName string, conditions []corev1.NodeCondition, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   nodeName,
			Labels: labels,
		},
		Status: corev1.NodeStatus{
			Conditions: conditions,
		},
	}
}

func nodeReadyTrueCondition() *corev1.NodeCondition {
	return &corev1.NodeCondition{
		Type:               corev1.NodeReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Date(2025, 7, 27, 12, 30, 0, 0, time.UTC),
		LastHeartbeatTime:  metav1.Date(2025, 7, 27, 12, 30, 0, 0, time.UTC),
	}
}

func nodeReadyFalseCondition() *corev1.NodeCondition {
	return &corev1.NodeCondition{
		Type:   corev1.NodeReady,
		Status: corev1.ConditionFalse,
	}
}

func watchdogCondition() *corev1.NodeCondition {
	return &corev1.NodeCondition{
		Type:   node.ConditionType,
		Status: corev1.ConditionTrue,
	}
}
