#!/usr/bin/env bash
# Regenerate the MachineConfig Helm template from machineconfig.bu.
# The Butane output is wrapped with Helm conditionals and templated values.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "${SCRIPT_DIR}/../chart" && pwd)"

raw="$(butane "${CHART_DIR}/machineconfig.bu")"

# Replace hardcoded name and role with Helm template values
raw="${raw/name: 99-worker-watchdog/name: {{ .Values.machineConfig.name }}}"
raw="${raw/machineconfiguration.openshift.io\/role: worker/machineconfiguration.openshift.io/role: {{ .Values.machineConfig.role }}}"

# Add common chart labels to metadata
raw="$(echo "$raw" | sed '/machineconfiguration.openshift.io\/role:/a\    {{- include "redfish-event-listener.labels" . | nindent 4 }}')"

cat > "${CHART_DIR}/templates/machineconfig.yaml" <<EOF
{{- /* Generated from machineconfig.bu by hack/gen-machineconfig.sh */ -}}
{{- if .Values.machineConfig.enabled }}
${raw}
{{- end }}
EOF

echo "Generated ${CHART_DIR}/templates/machineconfig.yaml"
