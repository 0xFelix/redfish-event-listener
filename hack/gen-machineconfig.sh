#!/usr/bin/env bash
# Regenerate the MachineConfig Helm template from machineconfig.bu.
# The Butane output is wrapped with Helm conditionals and templated values.
# Requires: butane, yq
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "${SCRIPT_DIR}/../chart" && pwd)"

raw="$(butane "${CHART_DIR}/machineconfig.bu")"

# Use yq for structural YAML modifications:
# - Replace hardcoded name and role with placeholders
# - Replace watchdogs.conf gzip+base64 content with a placeholder
# - Replace dracut entry source with a placeholder
raw="$(echo "$raw" | yq '
  .metadata.name = "__MC_NAME__" |
  .metadata.labels["machineconfiguration.openshift.io/role"] = "__MC_ROLE__" |
  (.spec.config.storage.files[] | select(.path == "/etc/modprobe.d/watchdogs.conf") | .contents) =
    {"compression": "", "source": "__WATCHDOGS_SOURCE__"} |
  (.spec.config.storage.files[] | select(.path == "/etc/dracut.conf.d/99-blacklist-watchdog.conf") | .contents.source) = "__DRACUT_SOURCE__"
')"

# Replace placeholders with Helm template expressions and insert conditionals.
# sed processes all -e expressions in order for each line, so the append (a\)
# commands can match text produced by earlier substitutions in the same pass.
raw="$(echo "$raw" | sed \
  -e 's/__MC_NAME__/{{ .Values.machineConfig.name }}/' \
  -e 's/__MC_ROLE__/{{ .Values.machineConfig.role }}/' \
  -e '/machineconfiguration.openshift.io\/role:/a\    {{- include "redfish-event-listener.labels" . | nindent 4 }}' \
  -e 's|__WATCHDOGS_SOURCE__|data:;base64,{{ include "redfish-event-listener.watchdogsConf" . \| b64enc }}|' \
  -e 's|__DRACUT_SOURCE__|data:;base64,{{ include "redfish-event-listener.dracutConf" . \| b64enc }}|')"

cat > "${CHART_DIR}/templates/machineconfig.yaml" <<EOF
{{- /* Generated from machineconfig.bu by hack/gen-machineconfig.sh */ -}}
{{- if .Values.machineConfig.enabled }}
${raw}
{{- end }}
EOF

echo "Generated ${CHART_DIR}/templates/machineconfig.yaml"
