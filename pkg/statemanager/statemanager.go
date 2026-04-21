package statemanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	core "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	state "github.com/0xfelix/redfish-event-listener/pkg/state/v1"
)

const stateDataKey = "state"

type StateManager interface {
	// GetNodeNameForToken returns the node name for a given authentication token
	GetNodeNameForToken(token string) (string, bool)

	// GetSubscriptions returns a copy of all current subscriptions
	GetSubscriptions() []state.Subscription

	// SetSubscriptions sets current subscriptions
	SetSubscriptions([]state.Subscription)

	// AddToManager initializes the StateManager with manager, registering controller and runnable
	AddToManager(mgr manager.Manager) error
}

type stateManager struct {
	lock            sync.RWMutex
	subs            []state.Subscription
	subsChanged     chan struct{}
	subsSetByMethod atomic.Bool

	tokenToName map[string]string

	secretName     string
	secretOwnerRef metav1.OwnerReference
	namespace      string
	client         client.Client
}

func New(secretName, namespace string, secretOwnerRef metav1.OwnerReference) StateManager {
	return &stateManager{
		subs:           []state.Subscription{},
		tokenToName:    map[string]string{},
		subsChanged:    make(chan struct{}, 1),
		secretName:     secretName,
		namespace:      namespace,
		secretOwnerRef: secretOwnerRef,
	}
}

func (s *stateManager) GetNodeNameForToken(token string) (string, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	nodeName, ok := s.tokenToName[token]
	return nodeName, ok
}

func (s *stateManager) GetSubscriptions() []state.Subscription {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return slices.Clone(s.subs)
}

func (s *stateManager) SetSubscriptions(subscriptions []state.Subscription) {
	s.setSubscriptionsInternal(subscriptions, false)
}

func (s *stateManager) setSubscriptionsInternal(subscriptions []state.Subscription, fromReconcile bool) {
	if fromReconcile && s.subsSetByMethod.Load() {
		// Subscriptions were previously set using the SetSubscriptions() method,
		// so we ignore any attempts to set the subscriptions from Reconcile.
		return
	}

	subsCopy := slices.Clone(subscriptions)
	slices.SortStableFunc(subsCopy, func(a, b state.Subscription) int {
		return strings.Compare(a.NodeConfig.NodeName, b.NodeConfig.NodeName)
	})

	s.lock.Lock()
	defer s.lock.Unlock()

	if fromReconcile && s.subsSetByMethod.Load() {
		return
	}

	if reflect.DeepEqual(s.subs, subsCopy) {
		return
	}

	s.subs = subsCopy
	s.tokenToName = make(map[string]string, len(s.subs))
	for _, sub := range s.subs {
		s.tokenToName[sub.Token] = sub.NodeConfig.NodeName
	}

	// Don't signal that state has changed, if it was loaded from Secret.
	if fromReconcile {
		return
	}

	s.subsSetByMethod.Store(true)

	// Non-blocking signal that state has changed
	select {
	case s.subsChanged <- struct{}{}:
	default:
	}
}

func (s *stateManager) AddToManager(mgr manager.Manager) error {
	s.client = mgr.GetClient()

	err := ctrl.NewControllerManagedBy(mgr).
		For(&core.Secret{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(object client.Object) bool {
			return object.GetName() == s.secretName && object.GetNamespace() == s.namespace
		}))).
		WithOptions(controller.Options{
			NeedLeaderElection: ptr.To(false),
		}).
		Complete(s)
	if err != nil {
		return fmt.Errorf("failed to add secret controller: %w", err)
	}

	// The Add() method adds the runnable as leader-elected by default.
	if err := mgr.Add(manager.RunnableFunc(s.runSecretWriter)); err != nil {
		return fmt.Errorf("failed to add secret writer runnable: %w", err)
	}

	return nil
}

func (s *stateManager) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	if request.Name != s.secretName || request.Namespace != s.namespace {
		return reconcile.Result{}, nil
	}

	// If the subscriptions were set using the method, it means that the FAR controller is running.
	// This Reconcile method needs to be disabled, so that it does not conflict with the FAR controller.
	if s.subsSetByMethod.Load() {
		return reconcile.Result{}, nil
	}

	secret := &core.Secret{}
	err := s.client.Get(ctx, request.NamespacedName, secret)
	if err != nil && !k8serrors.IsNotFound(err) {
		return reconcile.Result{}, fmt.Errorf("failed to get Secret: %w", err)
	}

	stateFromSecret, err := getStateFromSecret(secret)
	if err != nil {
		return reconcile.Result{}, err
	}

	s.setSubscriptionsInternal(stateFromSecret.Subscriptions, true)

	return reconcile.Result{}, nil
}

func (s *stateManager) runSecretWriter(ctx context.Context) error {
	backoff := wait.Backoff{
		Duration: 100 * time.Millisecond,
		Factor:   1.5,
		Steps:    math.MaxInt,
		Cap:      10 * time.Second,
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.subsChanged:
			if err := wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
				if err := s.writeState(ctx); err != nil {
					ctrl.LoggerFrom(ctx).Error(err, "failed to write state to Secret, retrying")
					return false, nil
				}
				return true, nil
			}); err != nil && !errors.Is(err, context.Canceled) {
				ctrl.LoggerFrom(ctx).Error(err, "failed to write state to Secret after retries")
			}
		}
	}
}

func (s *stateManager) writeState(ctx context.Context) error {
	stateObj := &state.State{
		Version:       state.VersionV1,
		Subscriptions: s.GetSubscriptions(),
	}

	secret := &core.Secret{}
	err := s.client.Get(ctx, client.ObjectKey{
		Name:      s.secretName,
		Namespace: s.namespace,
	}, secret)
	if k8serrors.IsNotFound(err) {
		newSecret := &core.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:            s.secretName,
				Namespace:       s.namespace,
				OwnerReferences: []metav1.OwnerReference{s.secretOwnerRef},
			},
		}
		if writeErr := writeStateToSecret(newSecret, stateObj); writeErr != nil {
			return fmt.Errorf("failed to serialize state for new Secret %s/%s: %w", s.namespace, s.secretName, writeErr)
		}
		return s.client.Create(ctx, newSecret)
	}
	if err != nil {
		return fmt.Errorf("failed to get Secret: %w", err)
	}

	if err = writeStateToSecret(secret, stateObj); err != nil {
		return fmt.Errorf("failed to serialize state for Secret %s/%s: %w", s.namespace, s.secretName, err)
	}

	return s.client.Update(ctx, secret)
}

func getStateFromSecret(secret *core.Secret) (*state.State, error) {
	stateBytes, ok := secret.Data[stateDataKey]
	if !ok {
		return &state.State{Version: state.VersionV1}, nil
	}

	sharedState := &state.State{}
	if err := json.Unmarshal(stateBytes, sharedState); err != nil {
		return nil, fmt.Errorf("failed to parse shared state from secret: %w", err)
	}

	if sharedState.Version != state.VersionV1 {
		return nil, fmt.Errorf("state stored in secret has unknown version: %s", sharedState.Version)
	}

	return sharedState, nil
}

func writeStateToSecret(secret *core.Secret, sharedState *state.State) error {
	if sharedState.Version != state.VersionV1 {
		return fmt.Errorf("state has unknown version: %s", sharedState.Version)
	}

	stateBytes, err := json.Marshal(sharedState)
	if err != nil {
		return fmt.Errorf("failed to serialize shared state: %w", err)
	}

	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	secret.Data[stateDataKey] = stateBytes
	return nil
}
