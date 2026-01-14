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

type Event struct {
	RedfishEvent redfish.Event
	NodeName     string
}

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
func HandleRedfishEvent(w http.ResponseWriter, r *http.Request, infoState *node.NodeInfoState, eventCh chan<- Event) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Limiting body to 1 MiB, so that a malicious request would not cause memory issues.
	// We do this, because authentication token is stored in the body,
	// and the request cannot be rejected based on headers only.
	const maxBodySize = 1024 * 1024
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	defer func() {
		if err := r.Body.Close(); err != nil {
			log.Printf("Error closing request body: %v", err)
		}
	}()

	var event redfish.Event
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&event); err != nil {
		log.Printf("Error decoding event: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	// The token is stored in the "Context" field in the request's body,
	// because the supermicro servers do not support setting custom HTTP headers for event subscriptions.
	token, hasPrefix := strings.CutPrefix(event.Context, common.EventContextPrefix)
	if !hasPrefix {
		log.Printf("Received event with invalid context: %q", event.Context)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	nodeName, err := lookupNodeNameFromToken(token, infoState)
	if err != nil {
		log.Printf("Authorization error: %v", err)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	eventCh <- Event{
		RedfishEvent: event,
		NodeName:     nodeName,
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("Event received")); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

func lookupNodeNameFromToken(token string, state *node.NodeInfoState) (string, error) {
	state.Lock.RLock()
	defer state.Lock.RUnlock()

	nodeName, ok := state.TokenToName[token]
	if !ok {
		return "", errors.New("invalid token")
	}

	return nodeName, nil
}

// HandleEvent logs the event details and invokes updateNodeCondition when a matching event is detected.
func HandleEvent(serverEvent *Event, k8sClient kubernetes.Interface) {
	event := serverEvent.RedfishEvent

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
			log.Printf("Detected watchdog reset event, updating node condition for %s", serverEvent.NodeName)
			if err := node.UpdateNodeCondition(k8sClient, serverEvent.NodeName); err != nil {
				log.Printf("Failed to update node condition: %v", err)
			} else {
				log.Printf("Successfully updated node condition for %s", serverEvent.NodeName)
			}
		}
	}
}
