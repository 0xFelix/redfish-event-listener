{{/*
Expand the name of the chart.
*/}}
{{- define "redfish-event-listener.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "redfish-event-listener.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "redfish-event-listener.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "redfish-event-listener.labels" -}}
helm.sh/chart: {{ include "redfish-event-listener.chart" . }}
{{ include "redfish-event-listener.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "redfish-event-listener.selectorLabels" -}}
app.kubernetes.io/name: {{ include "redfish-event-listener.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use.
*/}}
{{- define "redfish-event-listener.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "redfish-event-listener.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the secret to use.
*/}}
{{- define "redfish-event-listener.secretName" -}}
{{- default (printf "%s-credentials" (include "redfish-event-listener.fullname" .)) .Values.secret.name }}
{{- end }}

{{/*
Dracut configuration to omit the conflicting watchdog driver from initramfs.
*/}}
{{- define "redfish-event-listener.dracutConf" -}}
omit_drivers+=" {{ .Values.machineConfig.watchdogBlacklist }} "
{{ end -}}

{{/*
Watchdog modprobe configuration content.
The gen-machineconfig.sh script replaces the gzip+base64 Butane output for
/etc/modprobe.d/watchdogs.conf with this template so the blacklisted module
can be set via .Values.machineConfig.watchdogBlacklist.
*/}}
{{- define "redfish-event-listener.watchdogsConf" -}}
# Blacklist conflicting watchdog module
blacklist {{ .Values.machineConfig.watchdogBlacklist }}

# Set module options for ipmi_watchdog:
# action=reset: Perform a hard reset when timer expires.
# timeout=4: Set the hardware watchdog timeout to 4 seconds.
# panic_wdt_timeout=1: On a kernel panic, set the watchdog to reset the system after 1 second.
options ipmi_watchdog action=reset timeout=4 panic_wdt_timeout=1
{{ end -}}

{{/*
Create the container image reference.
*/}}
{{- define "redfish-event-listener.image" -}}
{{- $repository := required "image.repository is required" .Values.image.repository -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" $repository $tag }}
{{- end }}
