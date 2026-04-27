# Redfish Event Listener

Listens for Redfish BMC watchdog events and integrates with Kubernetes node
management.

## Prerequisites

- Kubernetes or OpenShift cluster with kubectl configured
- Podman or Docker installed locally
- Access to a container registry (e.g. quay.io)
- Redfish-enabled server/BMC
- Node Healthcheck Operator configured
- Fence Agents Remediation configured

### Configuring Node Healthcheck

Redfish Event Listener will set the node condition `RedfishWatchdogEvent` if an issue is detected,
Node HealthCheck should be configured to look for this particular unhealthy condition and status
`True`:

```yaml
apiVersion: remediation.medik8s.io/v1alpha1
kind: NodeHealthCheck
metadata:
  name: redfish-event-listener
spec:
  minHealthy: 51%
  remediationTemplate:
    apiVersion: fence-agents-remediation.medik8s.io/v1alpha1
    kind: FenceAgentsRemediationTemplate
    name: REPLACE_WITH_FAR_TEMPLATE_NAME
    namespace: REPLACE_WITH_FAR_NAMESPACE
  selector:
    matchExpressions:
      - key: node-role.kubernetes.io/worker
        operator: Exists
        values: []
  unhealthyConditions:
    - duration: 1s
      status: 'True'
      type: RedfishWatchdogEvent
```

Edit this example manifest replacing the placeholder values:

- `name`: Replace `REPLACE_WITH_FAR_TEMPLATE_NAME` with the name of the created
  FAR Template.
- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed.

More information about the configuration fields of Node Healthcheck can be located at [Node
Healthcheck documentation](https://github.com/medik8s/node-healthcheck-operator/blob/main/docs/configuration.md#spec-details).

## Setup Instructions

### 1. Build the Container Image

Build the container image using Podman:

```bash
podman build -t YOUR_IMAGE .
```

Replace `YOUR_IMAGE` with your actual image name, for example:

- `quay.io/yourusername/redfish-event-listener:latest`
- `docker.io/yourusername/redfish-event-listener:latest`

Push the image to your registry:

```bash
podman push YOUR_IMAGE
```

### 2. Deploy

Choose one of the following deployment methods.

#### Option A: Helm Chart

Requires Helm to be installed.

```bash
helm install redfish-event-listener chart/ \
  --namespace YOUR_NAMESPACE \
  --create-namespace \
  --set image.repository=YOUR_IMAGE_REPO \
  --set secret.destinationURL=https://events.example.com
```

All configurable values are documented in `chart/values.yaml`. Key values:

| Value | Description | Default |
|-------|-------------|---------|
| `image.repository` | Container image repository (required) | `""` |
| `image.tag` | Image tag | Chart `appVersion` |
| `secret.create` | Create the credentials Secret | `true` |
| `secret.insecure` | Skip TLS verification for Redfish | `false` |
| `secret.destinationURL` | External webhook URL | `""` |
| `ingress.enabled` | Enable Ingress | `false` |
| `ingress.host` | Ingress hostname (required when enabled) | `""` |
| `machineConfig.enabled` | Enable OpenShift IPMI watchdog MachineConfig | `false` |

To use an externally managed Secret instead of letting the chart create one:

```bash
helm install redfish-event-listener chart/ \
  --namespace YOUR_NAMESPACE \
  --set image.repository=YOUR_IMAGE_REPO \
  --set secret.create=false \
  --set secret.name=my-existing-secret
```

To enable Ingress for external access:

```bash
helm install redfish-event-listener chart/ \
  --namespace YOUR_NAMESPACE \
  --set image.repository=YOUR_IMAGE_REPO \
  --set ingress.enabled=true \
  --set ingress.host=events.example.com
```

#### Option B: Raw Manifests

The `manifests/` directory contains example files (`.example` suffix) that need
to be customized:

```bash
cp manifests/deployment.yaml.example manifests/deployment.yaml
cp manifests/secret.yaml.example manifests/secret.yaml
cp manifests/ingress.yaml.example manifests/ingress.yaml
cp manifests/rbac.yaml.example manifests/rbac.yaml
cp manifests/service.yaml.example manifests/service.yaml
```

**Important:** The copied files are ignored by git to prevent committing
sensitive data.

Edit each file and replace the placeholder values:

##### `rbac.yaml`

- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed

##### `service.yaml`

- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed

##### `secret.yaml`

- `insecure`: Set to `"true"` if using self-signed certificates
- `destinationURL`: The external URL where Redfish events will be sent (e.g.,
  `https://events.example.com`)
- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed

##### `deployment.yaml`

- `image`: Replace `REPLACE_WITH_YOUR_IMAGE` with your actual image name
- `replicas`: Adjust the number of replicas as needed (default is 2)
- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed

##### `ingress.yaml`

- `host`: Replace `REPLACE_WITH_YOUR_EXTERNAL_ROUTE` with your external URL
    - **Important:** This should match the `destinationURL` in `secret.yaml`
- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed

Deploy the manifests in the following order:

```bash
kubectl apply -f manifests/rbac.yaml
kubectl apply -f manifests/secret.yaml
kubectl apply -f manifests/deployment.yaml
kubectl apply -f manifests/service.yaml
kubectl apply -f manifests/ingress.yaml
```

### 3. MachineConfig (OpenShift Only)

**WARNING: Applying the MachineConfig will reboot all matching nodes!**

The MachineConfig loads the `ipmi_watchdog` kernel module and enables it
persistently across reboots.

When using the **Helm chart**, enable it with:

```bash
helm upgrade redfish-event-listener chart/ \
  --namespace YOUR_NAMESPACE \
  --reuse-values \
  --set machineConfig.enabled=true
```

When using **raw manifests**, apply it directly:

```bash
kubectl apply -f manifests/machineconfig.yaml
```

The `machineconfig.yaml` is generated from the `machineconfig.bu` Butane source.
To regenerate after editing the `.bu` file:

```bash
butane manifests/machineconfig.bu -o manifests/machineconfig.yaml
```

## Verification

### Check Deployment Status

```bash
# Helm
kubectl get deployment -l app.kubernetes.io/name=redfish-event-listener -n YOUR_NAMESPACE

# Raw manifests
kubectl get deployment redfish-event-listener -n YOUR_NAMESPACE
```

```bash
kubectl get node NODE_NAME -o yaml | grep -A 5 "type: RedfishWatchdogEvent"
```

## Cleanup

Helm:

```bash
helm uninstall redfish-event-listener --namespace YOUR_NAMESPACE
```

Raw manifests:

```bash
kubectl delete -f manifests/ingress.yaml
kubectl delete -f manifests/service.yaml
kubectl delete -f manifests/deployment.yaml
kubectl delete -f manifests/secret.yaml
kubectl delete -f manifests/rbac.yaml
```

**Note:** If MachineConfig was applied, removing it will also trigger node
reboots.
