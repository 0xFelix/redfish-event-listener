package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/stmcginnis/gofish"
	"github.com/stmcginnis/gofish/redfish"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	envNodeName       = "NODE_NAME"
	envDestinationURL = "DESTINATION_URL"

	envRedfishURL      = "REDFISH_URL"
	envRedfishUser     = "REDFISH_USER"
	envRedfishPass     = "REDFISH_PASS"
	envRedfishInsecure = "REDFISH_INSECURE"

	addr              = "0.0.0.0:8080"
	readHeaderTimeout = 10
	shutdownTimeout   = 5
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	k8sClient, err := createK8sClient()
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	nodeName := lookupEnv(envNodeName)
	log.Printf("Monitoring node: %s", nodeName)

	grp := sync.WaitGroup{}
	defer grp.Wait()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	eventCh := make(chan redfish.Event, 128)

	grp.Add(1)
	go func() {
		defer grp.Done()
		for {
			select {
			case event, ok := <-eventCh:
				if !ok {
					return
				}
				handleEvent(&event, k8sClient, nodeName)
			}
		}
	}()

	grp.Add(1)
	go func() {
		defer grp.Done()
		defer close(eventCh)
		err := runServer(ctx, func(w http.ResponseWriter, r *http.Request) {
			handleRedfishEvent(w, r, eventCh)
		})
		if err != nil {
			log.Printf("Error running server: %v", err)
		}
	}()

	subscription, err := createSubscription(lookupEnv(envDestinationURL))
	if err != nil {
		return fmt.Errorf("failed to create event subscription: %w", err)
	}
	log.Printf("Created Redfish event subscription: %s", subscription.ID)

	defer func() {
		log.Printf("Deleting Redfish event subscription: %s", subscription.ID)
		if err := deleteSubscription(subscription); err != nil {
			log.Print(err)
		}
	}()

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

func createK8sClient() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	return kubernetes.NewForConfig(config)
}

func createRedfishClient() (*gofish.APIClient, error) {
	config := gofish.ClientConfig{
		Endpoint:  lookupEnv(envRedfishURL),
		Username:  lookupEnv(envRedfishUser),
		Password:  lookupEnv(envRedfishPass),
		Insecure:  lookupInsecure(),
		BasicAuth: true,
	}

	return gofish.Connect(config)
}

func createSubscription(destinationURL string) (*redfish.EventDestination, error) {
	client, err := createRedfishClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Redfish client: %w", err)
	}
	defer client.Logout()

	service, err := client.GetService().EventService()
	if err != nil {
		return nil, fmt.Errorf("failed to get EventService: %w", err)
	}

	uri, err := redfish.CreateEventDestinationInstance(
		client, service.Subscriptions, destinationURL,
		nil, nil, nil,
		redfish.RedfishEventDestinationProtocol, "RedfishEventListener",
		redfish.RetryForeverDeliveryRetryPolicy, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create event subscription: %w", err)
	}

	dest, err := redfish.GetEventDestination(client, uri)
	if err != nil {
		return nil, fmt.Errorf("failed to get created event subscription: %w", err)
	}

	return dest, nil
}

func deleteSubscription(subscription *redfish.EventDestination) error {
	client, err := createRedfishClient()
	if err != nil {
		return fmt.Errorf("failed to create Redfish client: %w", err)
	}
	defer client.Logout()

	if err := redfish.DeleteEventDestination(client, subscription.ODataID); err != nil {
		return fmt.Errorf("failed to delete event subscription: %w", err)
	}

	return nil
}

func runServer(ctx context.Context, handler http.HandlerFunc) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handler)

	s := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout * time.Second,
	}

	errCh := make(chan error)
	go func() {
		defer close(errCh)
		log.Printf("Starting Redfish event listener on %s", addr)
		if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutting down Redfish event listener")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout*time.Second)
		defer cancel()
		if err := s.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("server shutdown error: %w", err)
		}
	case err := <-errCh:
		return err
	}
	return nil
}

func handleRedfishEvent(w http.ResponseWriter, r *http.Request, eventCh chan<- redfish.Event) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event redfish.Event
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&event); err != nil {
		log.Printf("Error decoding event: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	defer func() {
		if err := r.Body.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	eventCh <- event

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("Event received")); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

func handleEvent(event *redfish.Event, k8sClient *kubernetes.Clientset, nodeName string) {
	log.Printf("Received Redfish event:")
	log.Printf("  ID: %s", event.ID)
	log.Printf("  Name: %s", event.Name)
	log.Printf("  Context: %s", event.Context)
	log.Printf("  Number of events: %d", len(event.Events))

	for i, ev := range event.Events {
		log.Printf("  Event %d:", i+1)
		log.Printf("    EventType: %s", ev.EventType)
		log.Printf("    EventID: %s", ev.EventID)
		log.Printf("    Severity: %s", ev.Severity)
		log.Printf("    Message: %s", ev.Message)
		log.Printf("    MessageID: %s", ev.MessageID)
		log.Printf("    Timestamp: %s", ev.EventTimestamp)

		if strings.Contains(ev.MessageID, "ASR0001") {
			log.Printf("Detected ASR0001 event, updating node condition for %s", nodeName)
			if err := updateNodeCondition(k8sClient, nodeName); err != nil {
				log.Printf("Failed to update node condition: %v", err)
			} else {
				log.Printf("Successfully updated node condition for %s", nodeName)
			}
		}
	}
}

func updateNodeCondition(k8sClient *kubernetes.Clientset, nodeName string) error {
	const conditionType = "TestCondition"

	node, err := k8sClient.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node: %w", err)
	}

	now := metav1.Now()
	newCondition := corev1.NodeCondition{
		Type:               conditionType,
		Status:             corev1.ConditionFalse,
		LastHeartbeatTime:  now,
		LastTransitionTime: now,
		Reason:             "EventReceived",
		Message:            "Redfish event ASR0001 received",
	}

	conditionExists := false
	for i, condition := range node.Status.Conditions {
		if condition.Type == conditionType {
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

	return nil
}
