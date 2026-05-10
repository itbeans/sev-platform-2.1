{{/*
PodDisruptionBudget — guarantees at least one pod survives node drain / eviction.

Only rendered when replicaCount > 1 (a PDB on a single-replica Deployment would
block voluntary disruptions entirely, which is usually not desired).

Values consumed:
  .Values.replicaCount
*/}}
{{- define "ev-common.pdb" -}}
{{- if gt (int .Values.replicaCount) 1 }}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "ev-common.fullname" . }}
  labels:
    {{- include "ev-common.labels" . | nindent 4 }}
spec:
  minAvailable: 1
  selector:
    matchLabels:
      {{- include "ev-common.selectorLabels" . | nindent 6 }}
{{- end }}
{{- end }}
