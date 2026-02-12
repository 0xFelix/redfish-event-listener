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

package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/stmcginnis/gofish/redfish"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/0xfelix/redfish-event-listener/pkg/common"
	"github.com/0xfelix/redfish-event-listener/pkg/node"
	"github.com/0xfelix/redfish-event-listener/pkg/server"
	state "github.com/0xfelix/redfish-event-listener/pkg/state/v1"
)

var _ = Describe("Redish event server", func() {
	Context("handling functionality", func() {
		const (
			nodeName = "node01"
			token    = "TESTTOKEN1235421"
		)

		var infoState *node.NodeInfoState

		BeforeEach(func() {
			infoState = &node.NodeInfoState{
				Subs: map[string]state.Subscription{
					nodeName: {},
				},
				TokenToName: map[string]string{
					token: nodeName,
				},
			}
		})

		DescribeTable("should rejects invalid",
			func(makeReq func() *http.Request, expectedStatus int) {
				eventCh := make(chan server.Event, 1)
				rr := httptest.NewRecorder()
				req := makeReq()

				server.HandleRedfishEvent(rr, req, infoState, eventCh)

				Expect(rr.Code).To(Equal(expectedStatus))
				Expect(eventCh).NotTo(Receive())
			},
			Entry("non-POST requests",
				func() *http.Request { return httptest.NewRequest(http.MethodGet, "/", http.NoBody) },
				http.StatusMethodNotAllowed,
			),
			Entry("non-JSON requests",
				func() *http.Request { return httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not-json")) },
				http.StatusBadRequest,
			),
			Entry("too long requests",
				func() *http.Request {
					prefix := strings.NewReader(`{"Context":"`)
					sufix := strings.NewReader(`"}`)

					// 2 MiB of repeating string
					const contextStr = "repeating-context-"
					longContext := bytes.NewReader(bytes.Repeat([]byte(contextStr), (2*1024*1024)/len(contextStr)))

					return httptest.NewRequest(http.MethodPost, "/", io.MultiReader(prefix, longContext, sufix))
				},
				http.StatusBadRequest,
			),
			Entry("event context requests",
				func() *http.Request {
					ev := redfish.Event{Context: "wrong-ctx"}
					body, err := json.Marshal(ev)
					Expect(err).ToNot(HaveOccurred())
					return httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
				},
				http.StatusBadRequest,
			),
			Entry("unauthorized requests",
				func() *http.Request {
					ev := redfish.Event{Context: common.EventContextPrefix + "incorrect-token"}
					body, err := json.Marshal(ev)
					Expect(err).ToNot(HaveOccurred())
					return httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
				},
				http.StatusUnauthorized,
			),
		)

		It("should accept valid event and forwards it on channel", func() {
			ev := redfish.Event{
				Context: common.EventContextPrefix + token,
				Events: []redfish.EventRecord{
					{
						EventID:        "1",
						Message:        "test",
						MessageID:      "OTHER",
						EventType:      redfish.AlertEventType,
						EventTimestamp: time.Now().Format(time.RFC3339),
					},
				},
			}
			body, err := json.Marshal(ev)
			Expect(err).NotTo(HaveOccurred())

			eventCh := make(chan server.Event, 1)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			rr := httptest.NewRecorder()

			server.HandleRedfishEvent(rr, req, infoState, eventCh)
			Expect(rr.Code).To(Equal(http.StatusOK))

			b, err := io.ReadAll(rr.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(ContainSubstring("Event received"))

			var received server.Event
			Expect(eventCh).To(Receive(&received))
			Expect(received.NodeName).To(Equal(nodeName))
			Expect(received.RedfishEvent.Context).To(Equal(ev.Context))
			Expect(received.RedfishEvent.Events).To(HaveLen(1))
		})
	})

	Context("watchdog event handling", func() {
		DescribeTable("should set or not set a node condition and label based on watchdog event",
			func(messageID string, expectCondition bool) {
				const nodeName = "node-1"
				transitionTime := metav1.Now()
				cs := fake.NewClientset(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeName,
							Labels: map[string]string{
								"test": "false",
							},
						},
						Status: corev1.NodeStatus{
							Conditions: []corev1.NodeCondition{
								{
									Type:               corev1.NodeReady,
									Status:             corev1.ConditionFalse,
									LastTransitionTime: transitionTime,
								},
							},
						},
					},
				)
				ev := &server.Event{
					RedfishEvent: redfish.Event{
						Context: common.EventContextPrefix + "test-token",
						Events: []redfish.EventRecord{
							{
								EventID:   "sub-1",
								Message:   "something",
								MessageID: messageID,
								Severity:  "OK",
							},
						},
					},
					NodeName: nodeName,
				}

				server.HandleEvent(ev, cs)

				n, err := cs.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
				Expect(err).NotTo(HaveOccurred())

				condition := getNodeCondition(n.Status.Conditions, node.ConditionType)

				if expectCondition {
					Expect(condition).NotTo(BeNil(), "expected RedfishWatchdogEvent to be set for watchdog events")
					Expect(condition.Status).To(Equal(corev1.ConditionTrue))

					Expect(n.Labels).To(HaveKeyWithValue(node.WatchdogResetTimeLabel,
						transitionTime.Time.Format(node.WatchdogResetTimeLabelFmt)))
					// Make sure it does not delete other labels
					Expect(n.Labels).To(HaveLen(2))
				} else {
					Expect(condition).To(BeNil(), "expected RedfishWatchdogEvent to not be set for non-watchdog events")
					Expect(n.Labels).NotTo(HaveKey(node.WatchdogResetTimeLabel))
				}
			},
			Entry("should not set a node condition for non-watchdog events", "NOT_WATCHDOG", false),
			Entry("should set a node condition for watchdog events", "ASR0001", true),
		)
	})
})

func getNodeCondition(conditions []corev1.NodeCondition, conditionType string) *corev1.NodeCondition {
	for _, c := range conditions {
		if string(c.Type) == conditionType {
			return &c
		}
	}
	return nil
}
