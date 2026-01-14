package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

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

	infoState := &node.NodeInfoState{
		Infos:       map[string]node.NodeInfo{},
		TokenToName: map[string]string{},
	}

	defer func() {
		infoState.Lock.Lock()
		defer infoState.Lock.Unlock()
		for _, info := range infoState.Infos {
			if info.SubscriptionID != "" {
				log.Printf("Deleting Redfish event subscription: %s", info.SubscriptionID)
				if delErr := redfishlib.DeleteSubscription(info.SubscriptionID, &info.NodeConfig); delErr != nil {
					log.Print(delErr)
				}
			}
		}
		infoState.Infos = map[string]node.NodeInfo{}
		infoState.TokenToName = map[string]string{}
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	const eventChSize = 128
	eventCh := make(chan server.Event, eventChSize)

	grp.Add(1)
	go func() {
		defer grp.Done()
		for event := range eventCh {
			server.HandleEvent(&event, k8sClient)
		}
	}()

	grp.Add(1)
	go func() {
		defer grp.Done()
		defer close(eventCh)
		err := server.RunServer(ctx, func(w http.ResponseWriter, r *http.Request) {
			server.HandleRedfishEvent(w, r, infoState, eventCh)
		})
		if err != nil {
			log.Printf("Error running server: %v", err)
		}
	}()

	for _, config := range nodeConfigs {
		log.Printf("Monitoring node: %s", config.NodeName)

		// Use cryptographically random token.
		token := rand.Text()

		subscriptionID, err := redfishlib.CreateSubscription(destinationURL, &config, token)
		if err != nil {
			return fmt.Errorf("failed to create event subscription: %w", err)
		}

		infoState.Lock.Lock()
		infoState.Infos[config.NodeName] = node.NodeInfo{
			NodeConfig:     config,
			SubscriptionID: subscriptionID,
		}
		infoState.TokenToName[token] = config.NodeName
		infoState.Lock.Unlock()

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
