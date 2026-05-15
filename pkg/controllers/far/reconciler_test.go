package far_test

import (
	"context"
	stderrors "errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/0xfelix/redfish-event-listener/pkg/controllers/far"
	state "github.com/0xfelix/redfish-event-listener/pkg/state/v1"
	"github.com/0xfelix/redfish-event-listener/pkg/statemanager"
)

var _ = Describe("Reconciler", func() {
	const (
		destination       = "http://example.org/destination"
		namespace         = "namespace"
		farName           = "test-far"
		node1Name         = "node1"
		node2Name         = "node2"
		token1            = "token1"
		token2            = "token2"
		sub1              = "sub-1"
		sub2              = "sub-2"
		fenceIpmiLanAgent = "fence_ipmilan"
	)

	var (
		fakeClient    client.Client
		reconciler    reconcile.Reconciler
		stateMgr      *testStateManager
		createSubFunc far.CreateSubscriptionFunc
		deleteSubFunc far.DeleteSubscriptionFunc
	)

	BeforeEach(func() {
		fakeClient = fake.NewClientBuilder().Build()
		createSubFunc = nil
		deleteSubFunc = nil
		stateMgr = &testStateManager{
			State: &state.State{
				Version:       state.VersionV1,
				Subscriptions: []state.Subscription{},
			},
		}

		reconciler = far.NewReconciler(
			namespace,
			true,
			destination,
			fakeClient,
			stateMgr,
			func(destinationURL string, nodeConfig *state.NodeConfig, authToken string) (string, error) {
				if createSubFunc != nil {
					return createSubFunc(destinationURL, nodeConfig, authToken)
				}
				Fail("createSubscriptionFunc should not be called")
				return "", nil
			},
			func(subscriptionURI string, nodeConfig *state.NodeConfig) error {
				if deleteSubFunc != nil {
					return deleteSubFunc(subscriptionURI, nodeConfig)
				}
				Fail("deleteSubscriptionFunc should not be called")
				return nil
			},
		)
	})

	It("should ignore FAR templates in different namespaces", func() {
		_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "other-ns", Name: farName},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("should delete subscriptions when FAR template is deleted", func() {
		stateMgr.State.Subscriptions = []state.Subscription{
			createSubscription(node1Name, sub1, farName, token1),
			createSubscription(node2Name, sub2, farName, token2),
		}

		deleted := []string{}
		deleteSubFunc = func(subID string, _ *state.NodeConfig) error {
			deleted = append(deleted, subID)
			return nil
		}

		_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      farName,
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(ConsistOf(sub1, sub2))
		Expect(stateMgr.State.Subscriptions).To(BeEmpty())
	})

	DescribeTable("agent filtering",
		func(agent string) {
			stateMgr.State.Subscriptions = []state.Subscription{
				createSubscription(node1Name, sub1, farName, token1),
			}

			deleteCalled := false
			deleteSubFunc = func(subID string, _ *state.NodeConfig) error {
				Expect(subID).To(Equal(sub1))
				deleteCalled = true
				return nil
			}

			far := farTemplate(farName, namespace, agent, map[string]farNode{
				node1Name: {ip: "192.168.1.1", username: "user", password: "pass"},
			})
			Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      farName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(deleteCalled).To(BeTrue())
			Expect(stateMgr.State.Subscriptions).To(BeEmpty())
		},
		Entry("should skip FAR template without agent field", ""),
		Entry("should skip FAR template with non-fence_ipmilan agent", "fence_other"))

	Context("FAR template validation", func() {
		It("should return error when nodeparameters structure is invalid", func() {
			far := farTemplate(farName, namespace, fenceIpmiLanAgent, map[string]farNode{})
			unstructured.RemoveNestedField(far.Object, "spec", "template", "spec", "nodeparameters")
			Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      farName,
				},
			})
			Expect(err).To(MatchError(ContainSubstring("nodeparameters")))
		})

		It("should return error when required parameter fields are missing", func() {
			far := farTemplate(farName, namespace, fenceIpmiLanAgent, map[string]farNode{})
			nodeParams := map[string]interface{}{
				"--username": map[string]interface{}{node1Name: "user"},
				"--password": map[string]interface{}{node1Name: "pass"},
			}
			Expect(unstructured.SetNestedMap(far.Object, nodeParams, "spec", "template", "spec", "nodeparameters")).To(Succeed())
			Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      farName,
				},
			})

			Expect(err).To(MatchError(ContainSubstring("failed to find '--ip' parameter")))
		})

		DescribeTable("should skip nodes with missing",
			func(invalidNode farNode) {
				stateMgr.State.Subscriptions = []state.Subscription{
					createSubscription(node1Name, sub1, farName, token1),
				}

				var created []string
				createSubFunc = func(_ string, cfg *state.NodeConfig, _ string) (string, error) {
					created = append(created, cfg.NodeName)
					return "sub-" + cfg.NodeName, nil
				}

				var deleteCalled bool
				deleteSubFunc = func(subID string, _ *state.NodeConfig) error {
					Expect(subID).To(Equal(sub1))
					deleteCalled = true
					return nil
				}

				const node3Name = "node3"
				far := farTemplate(farName, namespace, fenceIpmiLanAgent, map[string]farNode{
					node2Name: invalidNode,
					node3Name: {ip: "192.168.1.3", username: "user", password: "pass"},
				})
				Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

				_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: namespace,
						Name:      farName,
					},
				})

				Expect(err).NotTo(HaveOccurred())
				Expect(created).To(ConsistOf(node3Name))
				Expect(deleteCalled).To(BeTrue())

				Expect(stateMgr.State.Subscriptions).To(HaveLen(1))
				Expect(stateMgr.State.Subscriptions[0].NodeConfig.NodeName).To(Equal(node3Name))
			},
			Entry("username", farNode{ip: "192.168.1.2", username: "", password: "pass"}),
			Entry("password", farNode{ip: "192.168.1.2", username: "user", password: ""}))
	})

	It("should create subscriptions", func() {
		var created []string
		createSubFunc = func(_ string, cfg *state.NodeConfig, token string) (string, error) {
			created = append(created, cfg.NodeName)
			Expect(token).NotTo(BeEmpty())
			return "sub-" + cfg.NodeName, nil
		}

		far := farTemplate(farName, namespace, fenceIpmiLanAgent, map[string]farNode{
			node1Name: {ip: "192.168.1.1", username: "user1", password: "pass1"},
			node2Name: {ip: "192.168.1.2", username: "user2", password: "pass2"},
		})
		Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

		_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      farName,
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(ConsistOf(node1Name, node2Name))

		resultState := stateMgr.State
		Expect(resultState.Subscriptions).To(HaveLen(2))

		subsByNode := map[string]state.Subscription{}
		for _, sub := range resultState.Subscriptions {
			subsByNode[sub.NodeConfig.NodeName] = sub
		}

		Expect(subsByNode).To(HaveKey(node1Name))
		Expect(subsByNode).To(HaveKey(node2Name))

		Expect(subsByNode[node1Name].FarTemplateName).To(Equal(farName))
		Expect(subsByNode[node1Name].Token).NotTo(BeEmpty())

		Expect(subsByNode[node2Name].FarTemplateName).To(Equal(farName))
		Expect(subsByNode[node2Name].Token).NotTo(BeEmpty())
	})

	It("should return error when subscription creation fails", func() {
		createErr := stderrors.New("subscription creation failed")
		createSubFunc = func(string, *state.NodeConfig, string) (string, error) {
			return "", createErr
		}

		far := farTemplate(farName, namespace, fenceIpmiLanAgent, map[string]farNode{
			node1Name: {ip: "192.168.1.1", username: "user", password: "pass"},
		})
		Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

		_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      farName,
			},
		})

		Expect(err).To(MatchError(createErr))

		resultState := stateMgr.State
		Expect(resultState.Subscriptions).To(HaveLen(1))
		Expect(resultState.Subscriptions[0].URI).To(BeEmpty())
	})

	Context("Subscription deletion", func() {
		It("should delete subscriptions for nodes removed from FAR spec", func() {
			stateMgr.State.Subscriptions = []state.Subscription{
				createSubscription(node1Name, sub1, farName, token1),
				createSubscription(node2Name, sub2, farName, token2),
			}

			createSubFunc = func(_ string, cfg *state.NodeConfig, _ string) (string, error) {
				if cfg.NodeName == node1Name {
					return sub1, nil
				}
				Fail("createSubscriptionFunc called for unexpected node: " + cfg.NodeName)
				return "", nil
			}

			deleteSubFunc = func(subID string, _ *state.NodeConfig) error {
				Expect(subID).To(Equal(sub2))
				return nil
			}

			far := farTemplate(farName, namespace, fenceIpmiLanAgent, map[string]farNode{
				node1Name: {ip: "192.168.1.1", username: "user", password: "pass"},
			})
			Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      farName,
				},
			})

			Expect(err).NotTo(HaveOccurred())

			resultState := stateMgr.State
			Expect(resultState.Subscriptions).To(HaveLen(1))
			Expect(resultState.Subscriptions[0].NodeConfig.NodeName).To(Equal(node1Name))
			Expect(resultState.Subscriptions[0].Token).To(Equal(token1))
		})

		It("should continue cleanup even if subscription deletion fails", func() {
			stateMgr.State.Subscriptions = []state.Subscription{
				createSubscription(node1Name, sub1, farName, token1),
				createSubscription(node2Name, sub2, farName, token2),
			}

			initialState := stateMgr.State
			infosLenBeforeDeletion := len(initialState.Subscriptions)

			deleteErr := stderrors.New("delete failed")
			deletionAttempts := 0
			deleteSubFunc = func(subID string, _ *state.NodeConfig) error {
				deletionAttempts++
				return deleteErr
			}

			// Reconciled FAR template does not exist
			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      farName,
				},
			})

			Expect(err).To(MatchError(deleteErr))
			Expect(deletionAttempts).To(Equal(infosLenBeforeDeletion))
			Expect(stateMgr.State.Subscriptions).To(BeEmpty())
		})

		It("should delete subscriptions when FAR template becomes irrelevant", func() {
			stateMgr.State.Subscriptions = []state.Subscription{
				createSubscription(node1Name, sub1, farName, token1),
			}

			var deleteCalled bool
			deleteSubFunc = func(subID string, _ *state.NodeConfig) error {
				Expect(subID).To(Equal(sub1))
				deleteCalled = true
				return nil
			}

			far := farTemplate(farName, namespace, "fence_other", map[string]farNode{
				node1Name: {ip: "192.168.1.1", username: "user", password: "pass"},
			})
			Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      farName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(deleteCalled).To(BeTrue())

			Expect(stateMgr.State.Subscriptions).To(BeEmpty())
		})
	})

	Context("Authentication and configuration", func() {
		It("should generate auth token and store in subscription", func() {
			const testNodeName = "test-node"
			createCalled := false
			createSubFunc = func(_ string, cfg *state.NodeConfig, token string) (string, error) {
				Expect(cfg.NodeName).To(Equal(testNodeName))
				Expect(token).NotTo(BeEmpty())
				createCalled = true
				return "sub-" + cfg.NodeName, nil
			}

			far := farTemplate(farName, namespace, fenceIpmiLanAgent, map[string]farNode{
				testNodeName: {ip: "192.168.1.1", username: "user", password: "pass"},
			})
			Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      farName,
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(createCalled).To(BeTrue())

			resultState := stateMgr.State
			Expect(resultState.Subscriptions).To(HaveLen(1))
			sub := resultState.Subscriptions[0]
			Expect(sub.Token).NotTo(BeEmpty())
			Expect(sub.NodeConfig.NodeName).To(Equal(testNodeName))
		})

		It("should construct HTTPS URL from IP in node config", func() {
			const testIP = "192.168.1.100"
			createCalled := false
			createSubFunc = func(_ string, cfg *state.NodeConfig, _ string) (string, error) {
				Expect(cfg.URL).To(Equal("https://" + testIP))
				createCalled = true
				return "sub-" + cfg.NodeName, nil
			}

			far := farTemplate(farName, namespace, fenceIpmiLanAgent, map[string]farNode{
				node1Name: {ip: testIP, username: "user", password: "pass"},
			})
			Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      farName,
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(createCalled).To(BeTrue())
		})

		It("should store tokens for all created nodes", func() {
			var created []string
			createSubFunc = func(_ string, cfg *state.NodeConfig, token string) (string, error) {
				Expect(token).NotTo(BeEmpty())
				created = append(created, cfg.NodeName)
				return "sub-" + cfg.NodeName, nil
			}

			far := farTemplate(farName, namespace, fenceIpmiLanAgent, map[string]farNode{
				node1Name: {ip: "192.168.1.1", username: "user1", password: "pass1"},
				node2Name: {ip: "192.168.1.2", username: "user2", password: "pass2"},
			})
			Expect(fakeClient.Create(context.Background(), far)).To(Succeed())

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      farName,
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(created).To(ConsistOf(node1Name, node2Name))

			resultState := stateMgr.State
			Expect(resultState.Subscriptions).To(HaveLen(2))

			for _, sub := range resultState.Subscriptions {
				Expect(sub.Token).NotTo(BeEmpty())
				Expect(sub.NodeConfig.NodeName).To(Or(Equal(node1Name), Equal(node2Name)))
			}
		})
	})
})

func createSubscription(nodeName, uri, farTemplateName, token string) state.Subscription {
	return state.Subscription{
		NodeConfig: state.NodeConfig{
			NodeName: nodeName,
		},
		URI:             uri,
		FarTemplateName: farTemplateName,
		Token:           token,
	}
}

type farNode struct {
	ip       string
	username string
	password string
}

func farTemplate(name, namespace, agent string, nodes map[string]farNode) *unstructured.Unstructured {
	ips := make(map[string]interface{})
	users := make(map[string]interface{})
	passwords := make(map[string]interface{})

	for nodeName, n := range nodes {
		if n.ip != "" {
			ips[nodeName] = n.ip
		}
		if n.username != "" {
			users[nodeName] = n.username
		}
		if n.password != "" {
			passwords[nodeName] = n.password
		}
	}

	obj := far.NewFarTemplateUnstructured()
	obj.SetName(name)
	obj.SetNamespace(namespace)

	if agent != "" {
		if err := unstructured.SetNestedField(obj.Object, agent, "spec", "template", "spec", "agent"); err != nil {
			panic(err)
		}
	}

	if len(ips) > 0 {
		nodeParams := map[string]interface{}{
			"--ip":       ips,
			"--username": users,
			"--password": passwords,
		}
		if err := unstructured.SetNestedMap(obj.Object, nodeParams, "spec", "template", "spec", "nodeparameters"); err != nil {
			panic(err)
		}
	}

	return obj
}

type testStateManager struct {
	State *state.State
}

var _ statemanager.StateManager = &testStateManager{}

func (t *testStateManager) GetNodeNameForToken(_ string) (string, bool) {
	Fail("unexpected call to GetNodeNameForToken")
	return "", false
}

func (t *testStateManager) GetSubscriptions() []state.Subscription {
	result := make([]state.Subscription, len(t.State.Subscriptions))
	copy(result, t.State.Subscriptions)
	return result
}

func (t *testStateManager) SetSubscriptions(subs []state.Subscription) {
	t.State.Subscriptions = subs
}

func (t *testStateManager) AddToManager(_ manager.Manager) error {
	Fail("unexpected call to AddToManager")
	return nil
}
