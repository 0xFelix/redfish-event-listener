package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/0xfelix/redfish-event-listener/pkg/controllers/far"
	nodecondition "github.com/0xfelix/redfish-event-listener/pkg/controllers/node_condition"
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
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	k8sConfig, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

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

	podNamespace := lookupEnv(envPodNamespace)

	nodeLabelSelector, err := createNodeLabelSelector()
	if err != nil {
		return err
	}

	mgr, err := ctrl.NewManager(k8sConfig, ctrl.Options{
		BaseContext: func() context.Context { return ctx },
		Cache: cache.Options{
			// Limiting watch to only certain resources.
			// The controller will start informer for watched resources, so we don't need to add them manually.
			ReaderFailOnMissingInformer: true,
			// Watch only resources in the same namespace as the pod
			DefaultNamespaces: map[string]cache.Config{
				podNamespace: {},
			},
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Node{}: {
					Label: nodeLabelSelector,
				},
			},
		},
		Client: client.Options{
			Cache: &client.CacheOptions{
				// Configure the client to use cache for getting Unstructured objects.
				// Other parts of the code already use the Informer in the Cache to watch Unstructured FAR templates.
				// So the client can also use the cache.
				Unstructured: true,
			},
		},
		// Disabling metrics server
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create controller manager: %w", err)
	}

	if err := addControllersToManager(mgr, infoState); err != nil {
		return fmt.Errorf("failed to add controllers to manager: %w", err)
	}

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

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("failed to start manager: %w", err)
	}

	grp.Wait()

	return nil
}

func createNodeLabelSelector() (labels.Selector, error) {
	labelReq, err := labels.NewRequirement(node.WatchdogResetTimeLabel, selection.Exists, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create label requirement: %w", err)
	}

	return labels.NewSelector().Add(*labelReq), nil
}

func addControllersToManager(mgr manager.Manager, infoState *node.NodeInfoState) error {
	if err := far.AddToManager(
		lookupEnv(envPodNamespace),
		lookupInsecure(),
		lookupEnv(envDestinationURL),
		infoState,
		redfishlib.CreateSubscription,
		redfishlib.DeleteSubscription,
		mgr,
	); err != nil {
		return fmt.Errorf("failed to complete controller: %w", err)
	}

	if err := nodecondition.AddToManager(mgr); err != nil {
		return fmt.Errorf("failed to add node condition controller: %w", err)
	}
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
