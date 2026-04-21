package statemanager

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	state "github.com/0xfelix/redfish-event-listener/pkg/state/v1"
)

var _ = Describe("StateManager", func() {
	const (
		secretName = "test-secret"
		namespace  = "test-namespace"
		node       = "node-name"
		token      = "token"
	)

	var (
		ownerRef metav1.OwnerReference
		sm       *stateManager
	)

	BeforeEach(func() {
		ownerRef = metav1.OwnerReference{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       "test-deployment",
			UID:        "test-deployment-uid",
		}
		sm = New(secretName, namespace, ownerRef).(*stateManager)
	})

	Context("GetNodeNameByToken", func() {
		It("should return node name for existing token", func() {
			sm.SetSubscriptions([]state.Subscription{{
				NodeConfig: state.NodeConfig{NodeName: node},
				Token:      token,
			}})

			nodeName, ok := sm.GetNodeNameForToken(token)
			Expect(ok).To(BeTrue())
			Expect(nodeName).To(Equal(node))
		})

		It("should return false for non-existing token", func() {
			nodeName, ok := sm.GetNodeNameForToken("nonexistent")
			Expect(ok).To(BeFalse())
			Expect(nodeName).To(BeEmpty())
		})
	})

	Context("get and set subscriptions", func() {
		It("should set subscriptions and rebuild token map", func() {
			subs := []state.Subscription{{
				NodeConfig: state.NodeConfig{NodeName: node},
				Token:      token,
			}, {
				NodeConfig: state.NodeConfig{NodeName: "node2"},
				Token:      "token2",
			}}

			sm.SetSubscriptions(subs)

			result := sm.GetSubscriptions()
			Expect(result).To(HaveLen(2))
			Expect(result).To(ContainElements(subs))

			nodeName1, ok1 := sm.GetNodeNameForToken(token)
			Expect(ok1).To(BeTrue())
			Expect(nodeName1).To(Equal(node))

			nodeName2, ok2 := sm.GetNodeNameForToken("token2")
			Expect(ok2).To(BeTrue())
			Expect(nodeName2).To(Equal("node2"))
		})

		It("should sort subscriptions by node name", func() {
			subs := []state.Subscription{
				{NodeConfig: state.NodeConfig{NodeName: "node3"}, Token: "token3"},
				{NodeConfig: state.NodeConfig{NodeName: node}, Token: token},
				{NodeConfig: state.NodeConfig{NodeName: "node2"}, Token: "token2"},
			}

			sm.SetSubscriptions(subs)

			result := sm.GetSubscriptions()
			Expect(result).To(HaveLen(3))
			Expect(result[0].NodeConfig.NodeName).To(Equal(node))
			Expect(result[1].NodeConfig.NodeName).To(Equal("node2"))
			Expect(result[2].NodeConfig.NodeName).To(Equal("node3"))
		})

		It("should handle updating subscriptions multiple times", func() {
			subs1 := []state.Subscription{
				{NodeConfig: state.NodeConfig{NodeName: node}, Token: token},
			}
			sm.SetSubscriptions(subs1)

			nodeName, ok := sm.GetNodeNameForToken(token)
			Expect(ok).To(BeTrue())
			Expect(nodeName).To(Equal(node))

			const newName = "node2"
			const newToken = "token2"
			subs2 := []state.Subscription{
				{NodeConfig: state.NodeConfig{NodeName: newName}, Token: newToken},
			}
			sm.SetSubscriptions(subs2)

			_, ok = sm.GetNodeNameForToken(token)
			Expect(ok).To(BeFalse())

			nodeName, ok = sm.GetNodeNameForToken(newToken)
			Expect(ok).To(BeTrue())
			Expect(nodeName).To(Equal(newName))
		})
	})

	Context("Reconcile", func() {
		var (
			ctx        context.Context
			fakeClient client.Client
		)

		BeforeEach(func() {
			ctx = context.Background()
			fakeClient = fake.NewClientBuilder().Build()
			sm.client = fakeClient
		})

		It("should ignore requests for different secret name", func() {
			initialSubs := []state.Subscription{{
				NodeConfig: state.NodeConfig{NodeName: node},
				Token:      token,
			}}
			sm.setSubscriptionsInternal(initialSubs, true)

			_, err := sm.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "other-secret",
					Namespace: namespace,
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(sm.GetSubscriptions()).To(Equal(initialSubs))
		})

		It("should ignore requests for different namespace", func() {
			initialSubs := []state.Subscription{{
				NodeConfig: state.NodeConfig{NodeName: node},
				Token:      token,
			}}
			sm.setSubscriptionsInternal(initialSubs, true)

			_, err := sm.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      secretName,
					Namespace: "other-namespace",
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(sm.GetSubscriptions()).To(Equal(initialSubs))
		})

		It("should clear subscriptions when secret is not found with existing state", func() {
			initialSubs := []state.Subscription{{
				NodeConfig: state.NodeConfig{NodeName: node},
				Token:      token,
			}}
			sm.setSubscriptionsInternal(initialSubs, true)

			_, err := sm.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      secretName,
					Namespace: namespace,
				},
			})

			Expect(err).NotTo(HaveOccurred())

			subs := sm.GetSubscriptions()
			Expect(subs).To(BeEmpty())

			_, ok := sm.GetNodeNameForToken(token)
			Expect(ok).To(BeFalse())
		})

		It("should clear subscriptions when secret has empty subscriptions", func() {
			initialSubs := []state.Subscription{{
				NodeConfig: state.NodeConfig{NodeName: node},
				Token:      token,
			}}
			sm.setSubscriptionsInternal(initialSubs, true)

			stateData := &state.State{
				Version:       state.VersionV1,
				Subscriptions: []state.Subscription{},
			}
			stateBytes, err := json.Marshal(stateData)
			Expect(err).NotTo(HaveOccurred())

			secret := &core.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: namespace,
				},
				Data: map[string][]byte{
					stateDataKey: stateBytes,
				},
			}
			Expect(fakeClient.Create(ctx, secret)).To(Succeed())

			_, err = sm.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      secretName,
					Namespace: namespace,
				},
			})

			Expect(err).NotTo(HaveOccurred())

			subs := sm.GetSubscriptions()
			Expect(subs).To(BeEmpty())

			_, ok := sm.GetNodeNameForToken(token)
			Expect(ok).To(BeFalse())
		})

		It("should update state from secret", func() {
			stateData := &state.State{
				Version: state.VersionV1,
				Subscriptions: []state.Subscription{{
					NodeConfig:      state.NodeConfig{NodeName: node},
					URI:             "uri1",
					FarTemplateName: "far1",
					Token:           token,
				}},
			}
			stateBytes, err := json.Marshal(stateData)
			Expect(err).NotTo(HaveOccurred())

			secret := &core.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: namespace,
				},
				Data: map[string][]byte{
					stateDataKey: stateBytes,
				},
			}
			Expect(fakeClient.Create(ctx, secret)).To(Succeed())

			_, err = sm.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      secretName,
					Namespace: namespace,
				},
			})

			Expect(err).NotTo(HaveOccurred())

			resultSubs := sm.GetSubscriptions()
			Expect(resultSubs).To(HaveLen(1))
			Expect(resultSubs[0]).To(Equal(stateData.Subscriptions[0]))

			nodeName, ok := sm.GetNodeNameForToken(token)
			Expect(ok).To(BeTrue())
			Expect(nodeName).To(Equal(node))
		})

		It("should return error for invalid JSON in secret", func() {
			secret := &core.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: namespace,
				},
				Data: map[string][]byte{
					stateDataKey: []byte("invalid json"),
				},
			}
			Expect(fakeClient.Create(ctx, secret)).To(Succeed())

			_, err := sm.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      secretName,
					Namespace: namespace,
				},
			})

			Expect(err).To(MatchError(ContainSubstring("failed to parse shared state")))
		})

		It("should return error for wrong version in secret", func() {
			stateData := &state.State{
				Version:       "v999",
				Subscriptions: []state.Subscription{},
			}
			stateBytes, err := json.Marshal(stateData)
			Expect(err).NotTo(HaveOccurred())

			secret := &core.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: namespace,
				},
				Data: map[string][]byte{
					stateDataKey: stateBytes,
				},
			}
			Expect(fakeClient.Create(ctx, secret)).To(Succeed())

			_, err = sm.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      secretName,
					Namespace: namespace,
				},
			})

			Expect(err).To(MatchError(ContainSubstring("unknown version")))
		})

		It("should return error when getting secret fails", func() {
			errorClient := &errorClient{
				Client: fakeClient,
				getErr: fmt.Errorf("internal error"),
			}
			sm.client = errorClient

			_, err := sm.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      secretName,
					Namespace: namespace,
				},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get Secret"))
		})

		It("should not update internal state from reconcile after SetSubscriptions is called", func() {
			initialSubs := []state.Subscription{{
				NodeConfig:      state.NodeConfig{NodeName: node},
				URI:             "uri1",
				FarTemplateName: "far1",
				Token:           token,
			}}
			sm.SetSubscriptions(initialSubs)

			const differentToken = "different-token"
			stateData := &state.State{
				Version: state.VersionV1,
				Subscriptions: []state.Subscription{{
					NodeConfig:      state.NodeConfig{NodeName: "different-node"},
					URI:             "different-uri",
					FarTemplateName: "different-far",
					Token:           differentToken,
				}},
			}
			stateBytes, err := json.Marshal(stateData)
			Expect(err).NotTo(HaveOccurred())

			secret := &core.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: namespace,
				},
				Data: map[string][]byte{
					stateDataKey: stateBytes,
				},
			}
			Expect(fakeClient.Create(ctx, secret)).To(Succeed())

			_, err = sm.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      secretName,
					Namespace: namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			resultSubs := sm.GetSubscriptions()
			Expect(resultSubs).To(Equal(initialSubs))

			nodeName, ok := sm.GetNodeNameForToken(token)
			Expect(ok).To(BeTrue())
			Expect(nodeName).To(Equal(node))

			_, ok = sm.GetNodeNameForToken(differentToken)
			Expect(ok).To(BeFalse())
		})
	})

	Context("Secret Writer", func() {
		var (
			ctx        context.Context
			fakeClient client.Client

			cancelCtx  context.CancelFunc
			writerDone chan struct{}
		)

		BeforeEach(func() {
			fakeClient = fake.NewClientBuilder().Build()
			sm.client = fakeClient

			ctx, cancelCtx = context.WithCancel(context.Background())
			writerDone = make(chan struct{})
			go func() {
				// runSecretWriter() does not return non-nil error
				_ = sm.runSecretWriter(ctx)
				close(writerDone)
			}()
		})

		AfterEach(func() {
			cancelCtx()
			Eventually(writerDone, 1*time.Second).Should(BeClosed())
		})

		waitForSubscriptionCount := func(expectedCount int) {
			Eventually(func(g Gomega) {
				secret := &core.Secret{}
				g.Expect(fakeClient.Get(ctx, client.ObjectKey{
					Name:      secretName,
					Namespace: namespace,
				}, secret)).To(Succeed())

				stateBytes, ok := secret.Data[stateDataKey]
				g.Expect(ok).To(BeTrue())

				var stateData state.State
				g.Expect(json.Unmarshal(stateBytes, &stateData)).To(Succeed())
				g.Expect(stateData.Subscriptions).To(HaveLen(expectedCount))
			}, 2*time.Second, 100*time.Millisecond).Should(Succeed())
		}

		It("should create secret when it doesn't exist", func() {
			subs := []state.Subscription{{
				NodeConfig:      state.NodeConfig{NodeName: node},
				URI:             "uri1",
				FarTemplateName: "far1",
				Token:           token,
			}}
			sm.SetSubscriptions(subs)

			Eventually(func(g Gomega) {
				secret := &core.Secret{}
				g.Expect(fakeClient.Get(ctx, client.ObjectKey{
					Name:      secretName,
					Namespace: namespace,
				}, secret)).To(Succeed())

				stateBytes, ok := secret.Data[stateDataKey]
				g.Expect(ok).To(BeTrue())

				var stateData state.State
				g.Expect(json.Unmarshal(stateBytes, &stateData)).To(Succeed())
				g.Expect(stateData.Version).To(Equal(state.VersionV1))
				g.Expect(stateData.Subscriptions).To(Equal(subs))
			}, 2*time.Second, 100*time.Millisecond).Should(Succeed())
		})

		It("should set ownerReference to the deployment", func() {
			subs := []state.Subscription{{
				NodeConfig:      state.NodeConfig{NodeName: node},
				URI:             "uri1",
				FarTemplateName: "far1",
				Token:           token,
			}}
			sm.SetSubscriptions(subs)

			Eventually(func(g Gomega) {
				secret := &core.Secret{}
				g.Expect(fakeClient.Get(ctx, client.ObjectKey{
					Name:      secretName,
					Namespace: namespace,
				}, secret)).To(Succeed())

				g.Expect(secret.OwnerReferences).To(HaveLen(1))
				g.Expect(secret.OwnerReferences).To(ContainElement(ownerRef))
			}, 2*time.Second, 100*time.Millisecond).Should(Succeed())
		})

		It("should update existing secret", func() {
			initialStateData := &state.State{
				Version: state.VersionV1,
				Subscriptions: []state.Subscription{{
					NodeConfig: state.NodeConfig{NodeName: "old-node"},
					Token:      "old-token",
				}},
			}
			initialStateBytes, err := json.Marshal(initialStateData)
			Expect(err).NotTo(HaveOccurred())

			initialSecret := &core.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: namespace,
				},
				Data: map[string][]byte{
					stateDataKey: initialStateBytes,
				},
			}
			Expect(fakeClient.Create(ctx, initialSecret)).To(Succeed())

			newSubs := []state.Subscription{{
				NodeConfig:      state.NodeConfig{NodeName: node},
				URI:             "uri1",
				FarTemplateName: "far1",
				Token:           token,
			}, {
				NodeConfig:      state.NodeConfig{NodeName: "node2"},
				URI:             "uri2",
				FarTemplateName: "far2",
				Token:           "token2",
			}}
			sm.SetSubscriptions(newSubs)

			waitForSubscriptionCount(2)

			secret := &core.Secret{}
			Expect(fakeClient.Get(ctx, client.ObjectKey{
				Name:      secretName,
				Namespace: namespace,
			}, secret)).To(Succeed())

			stateBytes, ok := secret.Data[stateDataKey]
			Expect(ok).To(BeTrue())

			var stateData state.State
			Expect(json.Unmarshal(stateBytes, &stateData)).To(Succeed())
			Expect(stateData.Subscriptions).To(HaveLen(2))
			Expect(stateData.Subscriptions).To(ContainElements(newSubs))
		})

		It("should handle multiple updates", func() {
			subs1 := []state.Subscription{
				{NodeConfig: state.NodeConfig{NodeName: node}, Token: token},
			}
			sm.SetSubscriptions(subs1)

			waitForSubscriptionCount(1)

			subs2 := []state.Subscription{
				{NodeConfig: state.NodeConfig{NodeName: node}, Token: token},
				{NodeConfig: state.NodeConfig{NodeName: "node2"}, Token: "token2"},
			}
			sm.SetSubscriptions(subs2)

			waitForSubscriptionCount(2)

			subs3 := []state.Subscription{
				{NodeConfig: state.NodeConfig{NodeName: "node2"}, Token: "token2"},
			}
			sm.SetSubscriptions(subs3)

			waitForSubscriptionCount(1)
		})

		It("should not write when subscriptions are set to the same value", func() {
			subs := []state.Subscription{
				{NodeConfig: state.NodeConfig{NodeName: node}, Token: token},
			}
			sm.SetSubscriptions(subs)

			var initialResourceVersion string
			Eventually(func(g Gomega) {
				secret := &core.Secret{}
				g.Expect(fakeClient.Get(ctx, client.ObjectKey{
					Name:      secretName,
					Namespace: namespace,
				}, secret)).To(Succeed())
				initialResourceVersion = secret.ResourceVersion
			}, 2*time.Second, 100*time.Millisecond).Should(Succeed())

			sm.SetSubscriptions(subs)

			Consistently(func(g Gomega) {
				secret := &core.Secret{}
				g.Expect(fakeClient.Get(ctx, client.ObjectKey{
					Name:      secretName,
					Namespace: namespace,
				}, secret)).To(Succeed())

				g.Expect(secret.ResourceVersion).To(Equal(initialResourceVersion))
			}, 500*time.Millisecond, 100*time.Millisecond).Should(Succeed())
		})
	})
})

type errorClient struct {
	client.Client
	getErr error
}

func (e *errorClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if e.getErr != nil {
		return e.getErr
	}
	return e.Client.Get(ctx, key, obj, opts...)
}
