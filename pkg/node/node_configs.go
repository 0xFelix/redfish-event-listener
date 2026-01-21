package node

import (
	"context"
	"fmt"
	"log"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type NodeConfig struct {
	NodeName string `json:"nodeName"`
	URL      string `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
	Insecure bool   `json:"insecure,omitempty"`
}

type NodeInfo struct {
	NodeConfig     NodeConfig
	SubscriptionID string
	FarObjName     string
	Token          string
}

type NodeInfoState struct {
	Lock        sync.RWMutex
	Infos       map[string]NodeInfo
	TokenToName map[string]string
}

func GetNodesConfigFromFARConfig(client dynamic.Interface, namespace string, insecure bool) ([]NodeConfig, error) {
	objList, err := client.Resource(schema.GroupVersionResource{
		Group:    "fence-agents-remediation.medik8s.io",
		Version:  "v1alpha1",
		Resource: "fenceagentsremediationtemplates",
	}).Namespace(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list FenceAgentsRemediationTemplates: %w", err)
	}

	var allNodeConfigs []NodeConfig
	for _, obj := range objList.Items {
		if obj.GetNamespace() != namespace {
			continue
		}

		agent, found, err := unstructured.NestedString(obj.Object, "spec", "template", "spec", "agent")
		if err != nil {
			return nil, fmt.Errorf("failed to get .spec.template.spec.agent: %w", err)
		}
		if !found {
			log.Printf("Skipped FenceAgentsRemediationTemplates \"%s/%s\", no agent is defined.", obj.GetNamespace(), obj.GetName())
			continue
		}

		// Ignoring other agents
		if agent != "fence_ipmilan" {
			log.Printf("Skipped FenceAgentsRemediationTemplates \"%s/%s\", because its agent is %q", obj.GetNamespace(), obj.GetName(), agent)
			continue
		}

		nodeConfigs, err := nodeConfigsFromFar(&obj, insecure)
		if err != nil {
			return nil, fmt.Errorf("failed to get node config from FenceAgentsRemediationTemplates: %w", err)
		}

		allNodeConfigs = append(allNodeConfigs, nodeConfigs...)
	}

	return allNodeConfigs, nil
}

func nodeConfigsFromFar(obj *unstructured.Unstructured, insecure bool) ([]NodeConfig, error) {
	nodeParameters, found, err := unstructured.NestedMap(obj.Object, "spec", "template", "spec", "nodeparameters")
	if !found {
		return nil, fmt.Errorf("failed to find .spec.template.spec.nodeparameters")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get .spec.template.spec.nodeparameters: %w", err)
	}

	ips, found, err := unstructured.NestedStringMap(nodeParameters, "--ip")
	if !found {
		return nil, fmt.Errorf("failed to find '--ip' parameter")
	}
	if err != nil {
		return nil, fmt.Errorf("error getting '--ip' parameter: %w", err)
	}
	users, found, err := unstructured.NestedStringMap(nodeParameters, "--username")
	if !found {
		return nil, fmt.Errorf("failed to find '--username' parameter")
	}
	if err != nil {
		return nil, fmt.Errorf("error getting '--username' parameter: %w", err)
	}
	passwords, found, err := unstructured.NestedStringMap(nodeParameters, "--password")
	if !found {
		return nil, fmt.Errorf("failed to find '--password' parameter")
	}
	if err != nil {
		return nil, fmt.Errorf("error getting '--password' parameter: %w", err)
	}

	var nodeConfigs []NodeConfig
	for nodeName, ip := range ips {
		user, ok := users[nodeName]
		if !ok {
			log.Printf("FAR config does not specify username for node %q, ignoring the node.", nodeName)
			continue
		}
		password, ok := passwords[nodeName]
		if !ok {
			log.Printf("FAR config does not specify password for node %q, ignoring the node.", nodeName)
			continue
		}
		nodeConfigs = append(nodeConfigs, NodeConfig{
			NodeName: nodeName,
			URL:      fmt.Sprintf("https://%s", ip),
			Username: user,
			Password: password,
			Insecure: insecure,
		})
	}

	return nodeConfigs, nil
}
