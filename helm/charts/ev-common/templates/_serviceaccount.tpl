{{/*
ServiceAccount — one dedicated SA per service so Istio can issue a unique
SPIFFE identity (cluster.local/ns/<ns>/sa/<name>) for mTLS peer authentication
and fine-grained AuthorizationPolicy rules.
*/}}
{{- define "ev-common.serviceaccount" -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "ev-common.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "ev-common.labels" . | nindent 4 }}
{{- end }}
