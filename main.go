package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/stmcginnis/gofish/redfish"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/0xfelix/redfish-event-listener/pkg/node"
	redfishlib "github.com/0xfelix/redfish-event-listener/pkg/redfish"
	"github.com/0xfelix/redfish-event-listener/pkg/server"
)

const (
	envDestinationURL  = "DESTINATION_URL"
	envPodNamespace    = "POD_NAMESPACE"
	envRedfishInsecure = "REDFISH_INSECURE"
	eventContextPrefix = "RedfishEventListener-"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// Ignoring linter, because we will change this function in future PRs.
func run() error { //nolint:funlen
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(k8sConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic Kubernetes client: %w", err)
	}

	nodeConfigs, err := node.GetNodesConfigFromFARConfig(dynamicClient, lookupEnv(envPodNamespace), lookupInsecure())
	if err != nil {
		return fmt.Errorf("failed read node configs: %w", err)
	}

	destinationURL := lookupEnv(envDestinationURL)

	grp := sync.WaitGroup{}
	defer grp.Wait()

	nodeInfoMap := map[string]node.NodeInfo{}
	nodeInfoMapLock := sync.RWMutex{}

	defer func() {
		nodeInfoMapLock.Lock()
		defer nodeInfoMapLock.Unlock()
		for _, info := range nodeInfoMap {
			if info.SubscriptionID != "" {
				log.Printf("Deleting Redfish event subscription: %s", info.SubscriptionID)
				if err := redfishlib.DeleteSubscription(info.SubscriptionID, &info.NodeConfig); err != nil {
					log.Print(err)
				}
			}
		}
		nodeInfoMap = map[string]node.NodeInfo{}
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	const eventChSize = 128
	eventCh := make(chan redfish.Event, eventChSize)

	grp.Add(1)
	go func() {
		defer grp.Done()
		for event := range eventCh {
			nodeName, ok := strings.CutPrefix(event.Context, eventContextPrefix)
			if !ok {
				log.Printf("Event does not have valid context: %s", event.Context)
				continue
			}

			nodeInfoMapLock.RLock()
			info, ok := nodeInfoMap[nodeName]
			nodeInfoMapLock.RUnlock()

			if !ok {
				log.Printf("Event node is not known: %s", nodeName)
				continue
			}

			server.HandleEvent(&event, k8sClient, info.NodeConfig.NodeName)
		}
	}()

	grp.Add(1)
	go func() {
		defer grp.Done()
		defer close(eventCh)
		err := server.RunServer(ctx, func(w http.ResponseWriter, r *http.Request) {
			server.HandleRedfishEvent(w, r, eventCh, eventContextPrefix)
		})
		if err != nil {
			log.Printf("Error running server: %v", err)
		}
	}()

	for _, config := range nodeConfigs {
		log.Printf("Monitoring node: %s", config.NodeName)

		subscriptionID, err := redfishlib.CreateSubscription(destinationURL, &config, eventContextPrefix+config.NodeName)
		if err != nil {
			return fmt.Errorf("failed to create event subscription: %w", err)
		}

		nodeInfoMapLock.Lock()
		nodeInfoMap[config.NodeName] = node.NodeInfo{
			NodeConfig:     config,
			SubscriptionID: subscriptionID,
		}
		nodeInfoMapLock.Unlock()

		log.Printf("Created Redfish event subscription: %s", subscriptionID)
	}

	grp.Wait()

	return nil
}

func lookupEnv(key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("Environment variable %s not set", key)
	}
	return val
}

func lookupInsecure() bool {
	val, ok := os.LookupEnv(envRedfishInsecure)
	if !ok {
		return false
	}
	insecure, err := strconv.ParseBool(val)
	if err != nil {
		log.Fatalf("Invalid value %s for environment variable REDFISH_INSECURE", val)
	}
	return insecure
}
