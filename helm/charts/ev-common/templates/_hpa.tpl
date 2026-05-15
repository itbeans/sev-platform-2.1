{{/*
HorizontalPodAutoscaler — CPU + memory metrics.

Values consumed:
  .Values.autoscaling.{enabled,minReplicas,maxReplicas,
                        targetCPUUtilizationPercentage,
                        targetMemoryUtilizationPercentage}  (default 80)
*/}}
{{- define "ev-common.hpa" -}}
{{- if .Values.autoscaling.enabled }}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ include "ev-common.fullname" . }}
  labels:
    {{- include "ev-common.labels" . | nindent 4 }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ include "ev-common.fullname" . }}
  minReplicas: {{ .Values.autoscaling.minReplicas }}
  maxReplicas: {{ .Values.autoscaling.maxReplicas }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type:               Utilization
          averageUtilization: {{ .Values.autoscaling.targetCPUUtilizationPercentage }}
    - type: Resource
      resource:
        name: memory
        target:
          type:               Utilization
          averageUtilization: {{ .Values.autoscaling.targetMemoryUtilizationPercentage | default 80 }}
{{- end }}
{{- end }}
