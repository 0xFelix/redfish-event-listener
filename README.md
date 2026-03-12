# Deployment Guide

This directory contains Kubernetes manifests for deploying the Redfish Event
Listener.

## Prerequisites

- Kubernetes or OpenShift cluster with kubectl configured
- Podman or Docker installed locally
- Access to a container registry (e.g. quay.io)
- Redfish-enabled server/BMC
- Node Healthcheck Operator configured
- Fence Agents Remediation configured

### Configuring Node Heathcheck

Redfish Event Listener will set the node condition `TestCondition` if an issue is detected,
Node HealthCheck should be configured to look for this particular unhealthy condition and status
`False`:

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
      status: 'False'
      type: TestCondition
```
Edit this example manifest replacing the placeholder values:

- `name`: Replace `REPLACE_WITH_FAR_TEMPLATE_NAME` with the name of the created
  FAR Template.
- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed.

More information about the configuration fields of Node Heathcheck can be located at [Node
Heathcheck documentation](https://github.com/medik8s/node-healthcheck-operator/blob/main/docs/configuration.md#spec-details).

## Setup Instructions

### 1. Build the Container Image

Build the container image using Podman:

```bash
podman build -t YOUR_IMAGE .
```

Replace `YOUR_IMAGE` with your actual image name, for example:

- `quay.io/yourusername/sbetest:latest`
- `docker.io/yourusername/sbetest:latest`

Push the image to your registry:

```bash
podman push YOUR_IMAGE
```

### 2. Prepare Manifest Files

The `manifests/` directory contains example files (`.example` suffix) that need
to be customized:

```bash
# Copy example files to actual manifest files
cp manifests/pod.yaml.example manifests/pod.yaml
cp manifests/secret.yaml.example manifests/secret.yaml
cp manifests/ingress.yaml.example manifests/ingress.yaml
cp manifests/rbac.yaml.example manifests/rbac.yaml
cp manifests/service.yaml.example manifests/service.yaml
```

**Important:** These files are ignored by git to prevent committing sensitive
data.

Edit each file and replace the placeholder values:

#### `rbac.yaml`

- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed

#### `service.yaml`

- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed

#### `secret.yaml`

- `insecure`: Set to `"true"` if using self-signed certificates
- `destinationURL`: The external URL where Redfish events will be sent (e.g.,
  `https://events.example.com`)
- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed

#### `pod.yaml`

- `image`: Replace `REPLACE_WITH_YOUR_IMAGE` with your actual image name
- Node affinity rule: Replace `REPLACE_WITH_NODE_TO_SCHEDULE_ON` with the
  node you want the Pod to be scheduled on
- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed

#### `ingress.yaml`

- `host`: Replace `REPLACE_WITH_YOUR_EXTERNAL_ROUTE` with your external URL
    - **Important:** This should match the `destinationURL` in `secret.yaml`
- `namespace`: Replace `REPLACE_WITH_FAR_NAMESPACE` with the namespace where
  FAR has been deployed

### 3. Deploy to Kubernetes

Deploy the manifests in the following order:

```bash
# 1. Create RBAC (ServiceAccount, ClusterRole, ClusterRoleBinding)
kubectl apply -f manifests/rbac.yaml

# 2. Create Secret with credentials
kubectl apply -f manifests/secret.yaml

# 3. Deploy the Pod
kubectl apply -f manifests/pod.yaml

# 4. Create the Service
kubectl apply -f manifests/service.yaml

# 5. Create the Ingress for external access
kubectl apply -f manifests/ingress.yaml
```

### 4. MachineConfig (OpenShift Only)

**WARNING:** The `machineconfig.yaml` is an OpenShift-specific resource that
enables the IPMI watchdog kernel module.

**⚠️ APPLYING THIS WILL REBOOT ALL MATCHING NODES! ⚠️**

This MachineConfig:

- Loads the `ipmi_watchdog` kernel module
- Enables it persistently across reboots
- **Will trigger a node reboot** when applied

#### Generating MachineConfig from Butane

The `machineconfig.yaml` file is generated from the `machineconfig.bu` Butane
configuration file. If you need to modify the MachineConfig, edit the `.bu` file
and regenerate the YAML:

```bash
# After updating machineconfig.bu, regenerate the YAML
butane manifests/machineconfig.bu -o manifests/machineconfig.yaml
```

**Note:** The YAML file is created from the Butane (.bu) source file. Always
edit the `.bu` file and regenerate, rather than editing the YAML directly.

#### Applying the MachineConfig

```bash
# Review the MachineConfig carefully before applying
kubectl apply -f manifests/machineconfig.yaml

# Monitor node reboots
kubectl get nodes -w
```

## Verification

### Check Pod Status

```bash
kubectl get pods -l app=redfish-event-listener -n NAMESPACE
kubectl logs -f redfish-event-listener -n NAMESPACE
```

### Check Node Conditions

When a watchdog reset event is received, the specified node condition will 
be updated:

```bash
kubectl get node <NODE_NAME> -o yaml | grep -A 5 "type: TestCondition"
```

## Cleanup

Remove all resources:

```bash
kubectl delete -f manifests/ingress.yaml
kubectl delete -f manifests/service.yaml
kubectl delete -f manifests/pod.yaml
kubectl delete -f manifests/secret.yaml
kubectl delete -f manifests/rbac.yaml
```

**Note:** Do not delete the MachineConfig unless you want to disable the IPMI
watchdog (this will also trigger node reboots).
