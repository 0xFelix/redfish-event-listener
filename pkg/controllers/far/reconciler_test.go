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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/0xfelix/redfish-event-listener/pkg/controllers/far"
	"github.com/0xfelix/redfish-event-listener/pkg/node"
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
		infoState     *node.NodeInfoState
		createSubFunc far.CreateSubscriptionFunc
		deleteSubFunc far.DeleteSubscriptionFunc
	)

	BeforeEach(func() {
		fakeClient = fake.NewFakeClient()
		infoState = &node.NodeInfoState{
			Infos:       make(map[string]node.NodeInfo),
			TokenToName: make(map[string]string),
		}
		createSubFunc = nil
		deleteSubFunc = nil

		reconciler = far.NewReconciler(
			namespace,
			true,
			destination,
			fakeClient,
			infoState,
			func(destinationURL string, nodeConfig *node.NodeConfig, authToken string) (string, error) {
				if createSubFunc != nil {
					return createSubFunc(destinationURL, nodeConfig, authToken)
				}
				Fail("createSubscriptionFunc should not be called")
				return "", nil
			},
			func(subscriptionURI string, nodeConfig *node.NodeConfig) error {
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
		addToInfoState(infoState, node1Name, sub1, farName, token1)
		addToInfoState(infoState, node2Name, sub2, farName, token2)

		deleted := []string{}
		deleteSubFunc = func(subID string, _ *node.NodeConfig) error {
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
		Expect(infoState.Infos).To(BeEmpty())
		Expect(infoState.TokenToName).To(BeEmpty())
	})

	DescribeTable("agent filtering",
		func(agent string) {
			addToInfoState(infoState, node1Name, sub1, farName, token1)

			deleteCalled := false
			deleteSubFunc = func(subID string, _ *node.NodeConfig) error {
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
			Expect(infoState.Infos).To(BeEmpty())
			Expect(infoState.TokenToName).To(BeEmpty())
		},
		Entry("should skip FAR template without agent field", ""),
		Entry("should skip FAR template with non-fence_ipmilan agent", "fence_other"),
	)

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
				addToInfoState(infoState, node1Name, sub1, farName, token1)

				var created []string
				createSubFunc = func(_ string, cfg *node.NodeConfig, _ string) (string, error) {
					created = append(created, cfg.NodeName)
					return "sub-" + cfg.NodeName, nil
				}

				var deleteCalled bool
				deleteSubFunc = func(subID string, _ *node.NodeConfig) error {
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
				Expect(infoState.Infos).To(HaveLen(1))
				Expect(infoState.Infos).To(HaveKey(node3Name))
			},
			Entry("username", farNode{ip: "192.168.1.2", username: "", password: "pass"}),
			Entry("password", farNode{ip: "192.168.1.2", username: "user", password: ""}),
		)
	})

	It("should create subscriptions", func() {
		var created []string
		createSubFunc = func(_ string, cfg *node.NodeConfig, token string) (string, error) {
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

		Expect(infoState.Infos).To(HaveLen(2))
		Expect(infoState.Infos).To(HaveKey(node1Name))
		Expect(infoState.Infos).To(HaveKey(node2Name))

		Expect(infoState.Infos[node1Name].FarObjName).To(Equal(farName))
		Expect(infoState.Infos[node1Name].Token).NotTo(BeEmpty())

		Expect(infoState.Infos[node2Name].FarObjName).To(Equal(farName))
		Expect(infoState.Infos[node2Name].Token).NotTo(BeEmpty())
	})

	It("should return error when subscription creation fails", func() {
		createErr := stderrors.New("subscription creation failed")
		createSubFunc = func(string, *node.NodeConfig, string) (string, error) {
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
		Expect(infoState.Infos).To(BeEmpty())
	})

	Context("Subscription deletion", func() {
		It("should delete subscriptions for nodes removed from FAR spec", func() {
			addToInfoState(infoState, node1Name, sub1, farName, token1)
			addToInfoState(infoState, node2Name, sub2, farName, token2)

			deleteSubFunc = func(subID string, _ *node.NodeConfig) error {
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
			Expect(infoState.Infos).To(HaveLen(1))
			Expect(infoState.Infos).To(HaveKey(node1Name))
			Expect(infoState.TokenToName).To(HaveLen(1))
			Expect(infoState.TokenToName).To(HaveKey(token1))
		})

		It("should continue cleanup even if subscription deletion fails", func() {
			addToInfoState(infoState, node1Name, sub1, farName, token1)
			addToInfoState(infoState, node2Name, sub2, farName, token2)

			infosLenBeforeDeletion := len(infoState.Infos)

			deleteErr := stderrors.New("delete failed")
			deletionAttempts := 0
			deleteSubFunc = func(subID string, _ *node.NodeConfig) error {
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
			Expect(infoState.Infos).To(BeEmpty())
			Expect(infoState.TokenToName).To(BeEmpty())
		})

		It("should delete subscriptions when FAR template becomes irrelevant", func() {
			addToInfoState(infoState, node1Name, sub1, farName, token1)

			var deleteCalled bool
			deleteSubFunc = func(subID string, _ *node.NodeConfig) error {
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
			Expect(infoState.Infos).To(BeEmpty())
			Expect(infoState.TokenToName).To(BeEmpty())
		})
	})

	Context("Authentication and configuration", func() {
		It("should generate auth token and store in TokenToName mapping", func() {
			const testNodeName = "test-node"
			createCalled := false
			createSubFunc = func(_ string, cfg *node.NodeConfig, token string) (string, error) {
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

			nodeInfo := infoState.Infos[testNodeName]
			Expect(nodeInfo.Token).NotTo(BeEmpty())
			Expect(infoState.TokenToName).To(HaveKey(nodeInfo.Token))
			Expect(infoState.TokenToName[nodeInfo.Token]).To(Equal(testNodeName))
		})

		It("should construct HTTPS URL from IP in node config", func() {
			const testIP = "192.168.1.100"
			createCalled := false
			createSubFunc = func(_ string, cfg *node.NodeConfig, _ string) (string, error) {
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
			createSubFunc = func(_ string, cfg *node.NodeConfig, token string) (string, error) {
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

			Expect(infoState.TokenToName).To(HaveLen(2))
			for nodeName, nodeInfo := range infoState.Infos {
				Expect(nodeInfo.Token).NotTo(BeEmpty())
				Expect(infoState.TokenToName).To(HaveKey(nodeInfo.Token))
				Expect(infoState.TokenToName[nodeInfo.Token]).To(Equal(nodeName))
			}
		})
	})
})

func addToInfoState(infoState *node.NodeInfoState, nodeName, subID, farName, token string) {
	infoState.Infos[nodeName] = node.NodeInfo{
		NodeConfig:     node.NodeConfig{NodeName: nodeName},
		SubscriptionID: subID,
		FarObjName:     farName,
		Token:          token,
	}
	infoState.TokenToName[token] = nodeName
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
