# AGENTS.md - redfish-event-listener

## Strict rules
- Always make sure Apache 2.0 headers are present.
- Always run `make all` before pushing.
- Always run `make vendor` before commit changes.

## Project Overview

redfish-event-listener is a Kubernetes tool that detects Redfish watchdog reset events in a bare metal cluster. The node condition `RedfishWatchdogEvent` will be set if an issue is detected. The Node HealthCheck operator (NHC) should be configured to look for this particular node condition and status `True`. NHC is responsible for triggering remediation actions using Fence Agent Remediation (FAR).

## Architecture

This project contains a single main binary (`main.go`), responsible for reading the user's configuration, running controllers and an HTTP server. It uses the [gofish](https://github.com/stmcginnis/gofish) library as the Redfish client.

The key directories are:

- `chart/`
  - Contains a Helm chart for deploying redfish-event-listener.
  - Responsibility: provide a templated, configurable alternative to raw manifests with optional Ingress and OpenShift MachineConfig support.

- `pkg/common/`
  - Contains shared constants used across packages.
  - Responsibility: define cross-cutting values to keep behavior consistent between server and Redfish integration code.
  - Key constant: `EventContextPrefix = "REL-token-"` — used to prefix authentication tokens stored in the Redfish event `Context` field.

- `pkg/controllers/`
  - Contains Kubernetes controller-runtime controllers that reconcile cluster resources.
  - Responsibility: watch Kubernetes objects and keep subscription/condition state aligned with the desired cluster state.

- `pkg/controllers/far/`
    - Contains the controller for `FenceAgentsRemediationTemplate` resources.
    - Responsibility: parse FAR node parameters, create/delete Redfish subscriptions per node, and synchronize in-memory node/token/subscription state.

- `pkg/controllers/node_condition/`
    - Contains the controller for Kubernetes `Node` objects and watchdog node condition cleanup logic. It has a cache configured to store `Node` objects with the label `redfish.event.listener/last-watchdog-reset-time`, which is set when a Redfish watchdog reset event is received.
    - Responsibility: remove `RedfishWatchdogEvent` node condition and label `redfish.event.listener/last-watchdog-reset-time` after the node becomes `Ready` again with a newer transition time.

- `pkg/node/`
  - Contains node-related domain models and Kubernetes node update logic.
  - Responsibility: define node configuration/state structures (`NodeConfig`, `NodeInfo`, `NodeInfoState`) and applies `RedfishWatchdogEvent` node condition and set the `redfish.event.listener/last-watchdog-reset-time` label on target nodes.

- `pkg/redfish/`
  - Contains Redfish API integration and event classification logic.
  - Responsibility: connect to BMC Redfish endpoints, manage event subscription lifecycle, handle vendor-specific subscription behavior, including fallback patching required by certain vendors such as SuperMicro, and identify watchdog reset message IDs.
  - `pkg/redfish/wrapper/`
    - Contains wrapper interfaces and patch payload types around gofish `EventService`.
    - Responsibility: abstract vendor/API details for easier testing and provide a focused contract for subscription create, read, update, delete, and patch operations.

- `pkg/state/`
  - Contains versioned data model types for persisted state (currently `v1`).
  - Responsibility: define the structures for subscription state, node configuration, and the top-level state object that is serialized into the Kubernetes `Secret`.

- `pkg/statemanager/`
  - Contains the central in-memory store for subscription state and token-to-node-name lookups, with persistence to a Kubernetes `Secret`.
  - Responsibility: provide thread-safe access to subscription state, restore state from the Secret on startup, and persist state changes made by the FAR controller back to the Secret.

- `pkg/server/`
  - Contains the HTTP listener server implementation for receiving Redfish events.
  - Responsibility: run/shutdown the HTTP server, validate incoming event context tokens, map events to nodes, and trigger node condition updates when watchdog reset events are detected.

### Token and Authentication Mechanism

Tokens authenticate incoming Redfish events. When a subscription is created, a cryptographically random token (generated with `crypto/rand.Text()`) is stored in the Redfish event subscription's `Context` field, prefixed with `REL-token-`. The token is stored in the `Context` field instead of HTTP headers because some BMC vendors (notably SuperMicro) do not support custom HTTP headers on event subscriptions. When an event arrives, the server extracts the token from the event body's `Context` field and looks up the associated node name via the `TokenToName` map.

### Vendor-Specific Behavior

#### Subscription creation fallback (SuperMicro)

When `CreateEventSubscription` returns HTTP `405 Method Not Allowed`, the code falls back to patching a predefined (pre-existing) event subscription. SuperMicro H/X12 BMCs ship with a fixed set of event destination slots; the code finds an unused slot (destination `"0.0.0.0"` or empty) and patches it with the destination URL, context token, and a SuperMicro-specific OEM payload that enables the subscription.

#### Watchdog reset message IDs

Each vendor uses a different `MessageId` to report watchdog timer resets:

| Vendor | MessageId |
|---|---|
| Dell | `ASR0001` |
| HPE | `IPMIWatchdogTimerReset` |
| SuperMicro | `0xc804ff` |
| Lenovo | `FQXSPWD0004I` |

These are matched via regex in `pkg/redfish/watchdog_events.go`.

### Label Time Format

The label `redfish.event.listener/last-watchdog-reset-time` stores timestamps in the format `2006-01-02T15-04-05Z` (hyphens instead of colons in the time portion). This is because Kubernetes label values only allow `[A-Za-z0-9_.-]` — colons are invalid.

## Build and Development
### Prerequisites

- Go (check `go.mod` for the required version).
- Kubernetes or OpenShift cluster with `kubectl` configured.
- Podman (preferred) or Docker installed locally.
- Access to a container registry (e.g. quay.io).
- Redfish-enabled server/BMC.
- Node HealthCheck Operator configured.
- Fence Agents Remediation configured.
  
### Building binaries
```
make build - Build the tool using local Go
podman build -t repository-url:tag -f Containerfile . - Build and create an image
podman push repository-url:tag - Push the image to a repository
```

### Key make targets

Run `make all` as the pre-commit validation step; it formats, vets, vendors, lints, and runs tests.

```
make all    - fmt, vet, vendor, lint, and tests
make test   - Unit tests
make lint   - golangci-lint
make fmt    - gofumpt formatting
make vet    - vet
make vendor - tidy and vendor
```

### Container image

The `Containerfile` uses a multi-stage build: Go (check the `Containerfile` for the current Go version) builder with `CGO_ENABLED=0`, then `gcr.io/distroless/static:nonroot` as the runtime base. The binary runs as non-root user `65532`.

### Deployment

Follow the steps described in [README](README.md) for deploying redfish-event-listener.
Note that for development purposes, it is better to keep the `insecure` parameter set to `true`.

### Helm chart

The `chart/` directory contains a Helm chart as an alternative to the raw manifests in `manifests/`.

Key templates:
- `deployment.yaml` - main workload with configurable replicas, security context, and pod anti-affinity.
- `secret.yaml` - credentials secret with `destinationURL`, `insecure`, and `host/port` fields.
- `ingress.yaml` - optional Ingress; TLS is always included (full entry with `secretName` or empty `- {}`).
- `machineconfig.yaml` - optional OpenShift MachineConfig for IPMI watchdog setup. Generated from `chart/machineconfig.bu` (Butane source) via `hack/gen-machineconfig.sh`.
- RBAC templates - ServiceAccount, ClusterRole/ClusterRoleBinding for node access, and namespace-scoped Role/RoleBinding for FAR template access.

The Butane source (`chart/machineconfig.bu`) defines the watchdog kernel module config. To regenerate the MachineConfig template after editing the Butane source, run `hack/gen-machineconfig.sh` (requires the `butane` CLI).

`values.yaml` documents all configurable parameters with Helm-doc comments (`# --`).

### CI Pipeline

GitHub Actions (`.github/workflows/ci.yaml`) runs on every push to `main` and every PR. Four parallel jobs:

1. **unit-test** — `make test`
2. **check-commited-files** — `make vendor` + `make fmt`, then verifies the git tree is clean (catches unformatted code or stale vendor).
3. **lint** — `make lint`
4. **build** — `make build`

## Testing

- **Unit test**: standard Go test + Ginkgo/Gomega. Run with `make test`.

### Test patterns

- Each package has a `suite_test.go` with `BeforeSuite`/`AfterSuite` for envtest setup.
- Use the fake client from package `sigs.k8s.io/controller-runtime/pkg/client/fake` when required.
- Prefer `DescribeTable` with `Entry` for parameterized test cases.

## Linting

- **golangci-lint** (version managed in Makefile) with `.golangci.yml` - line length 140, cyclomatic complexity 15, function length 100 lines / 50 statements.
- **gofumpt** - formatting (stricter than gofmt).

## Controller Design

### `pkg/controllers/node_condition/`

This controller watches `Node` objects and reconciles only when the custom condition `RedfishWatchdogEvent=True` exists. The controller has a `Node` cache set to store only `Node` objects with the label `redfish.event.listener/last-watchdog-reset-time`. Its purpose is to automatically clean up the watchdog condition and label after the node is healthy again.

Overall flow:

1. Fetch node from API server.
2. Check if the controller should start a reconciliation loop, i.e., the node condition `RedfishWatchdogEvent` is present and `True`.
3. Parse the watchdog reset timestamp label.
4. Read `NodeReady` condition.
5. If `NodeReady=True` and transition time is newer than the watchdog reset timestamp stored in the label, remove label and custom condition.
6. Patch node once on function exit.

### `pkg/controllers/far/`

This controller watches `FenceAgentsRemediationTemplate` (FAR) resources and manages Redfish event subscriptions for each node defined in the template. It reconciles only when the `.spec` of a FAR template changes. Concurrency is limited to 1 because the reconciler shares mutable subscription state.

Overall flow:

1. Fetch the `FenceAgentsRemediationTemplate` from the API server.
2. If the resource was deleted, delete all Redfish subscriptions associated with it and update the state manager.
3. Check if the FAR template is relevant — it must use the `fence_ipmilan` agent; templates with other agents are ignored.
4. If the template is no longer relevant, delete any previously created subscriptions for it.
5. Parse node parameters (`--ip`, `--username`, `--password`) from `.spec.template.spec.nodeparameters` to build a node configuration per node.
6. Diff the desired node set against the current subscriptions:
   - Nodes no longer present in the template → delete their Redfish subscription.
   - Nodes already subscribed → keep them.
   - New nodes → generate a cryptographic token and create a Redfish event subscription.
7. Persist the updated subscription list via the state manager.

### `pkg/statemanager/`

The state manager is the central in-memory store for subscription state and the token-to-node-name lookup map. It persists state to a Kubernetes `Secret` and restores it on startup, enabling replicas to share state and survive restarts.

It fulfills three roles:

1. **In-memory state + token lookup** — holds the current subscription list and a derived token-to-name map. The HTTP server uses the token lookup to authenticate incoming Redfish events. Access is protected by a read-write mutex.
2. **Secret reconciler** — a controller that watches the state `Secret`. On startup (before the FAR controller has run), it loads subscriptions from the Secret into memory. Once the FAR controller sets subscriptions, the reconciler disables itself to avoid overwriting live state.
3. **Secret writer** — a leader-elected runnable that listens for subscription changes. Whenever the FAR controller updates subscriptions, the writer persists the new state to the Secret with exponential backoff retry.

### Controller interaction flow

The FAR controller and the state manager work together in the following sequence:

1. **Startup / replica join** — The state manager reconciler reads the state `Secret` and populates the in-memory subscription list and token map. This allows the HTTP server to authenticate events immediately, before the FAR controller runs.
2. **FAR reconciliation** — When a `FenceAgentsRemediationTemplate` changes, the FAR controller gets the current subscriptions from the state manager, reconciles them against the desired node set, and writes the result back.
3. **State persistence** — The state manager updates the in-memory state and token map, disables the Secret reconciler (to avoid conflicts), and signals the writer.
4. **Secret write** — The writer persists the updated state to the Kubernetes `Secret`, making it available to other replicas.
5. **Event authentication** — When the HTTP server receives a Redfish event, it resolves the token from the event `Context` field to a node name via the state manager, then triggers the node condition update.

## Conventions

- All source files carry Apache 2.0 license headers.
- Commit messages: conventional commits with scope, e.g. feat(redfish): ..., fix(events): ...
- Commits must be signed off (git commit -s).
- If an AI agent assisted the developer, the commit must add `assisted-by` and the AI agent signature.
- PRs and issues must follow the GitHub templates in `.github/` (`.github/PULL_REQUEST_TEMPLATE.md`, `.github/ISSUE_TEMPLATE.md`). Always read the template before creating a PR or issue and fill in all sections.

