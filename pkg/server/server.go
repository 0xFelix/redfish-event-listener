package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/stmcginnis/gofish/redfish"
	"k8s.io/client-go/kubernetes"

	"github.com/0xfelix/redfish-event-listener/pkg/common"
	"github.com/0xfelix/redfish-event-listener/pkg/node"
	redfishlib "github.com/0xfelix/redfish-event-listener/pkg/redfish"
)

const (
	addr              = "0.0.0.0:8080"
	readHeaderTimeout = 10
	shutdownTimeout   = 5
)

// RunServer starts an HTTP server on the provided address using the given handler.
// It sets the read header timeout and performs a graceful shutdown when the context is canceled.
func RunServer(ctx context.Context, handler http.HandlerFunc) error {
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

// HandleRedfishEvent decodes a Redfish Event from the request, validates its context,
// and sends it to the provided channel.
func HandleRedfishEvent(w http.ResponseWriter, r *http.Request, eventCh chan<- redfish.Event) {
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

	if !strings.HasPrefix(event.Context, common.EventContextPrefix) {
		log.Printf("Received event with invalid context: %q", event.Context)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	eventCh <- event

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("Event received")); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

// HandleEvent logs the event details and invokes updateNodeCondition when a matching event is detected.
func HandleEvent(event *redfish.Event, k8sClient kubernetes.Interface, nodeName string) {
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

		if redfishlib.IsWatchdogResetEvent(ev.MessageID) {
			log.Printf("Detected watchdog reset event, updating node condition for %s", nodeName)
			if err := node.UpdateNodeCondition(k8sClient, nodeName); err != nil {
				log.Printf("Failed to update node condition: %v", err)
			} else {
				log.Printf("Successfully updated node condition for %s", nodeName)
			}
		}
	}
}
