{{/*
Standard ClusterIP Service for an ev-server microservice.

Ports exposed:
  http     — .Values.service.httpPort  (always present)
  grpc     — .Values.service.grpcPort  (only when non-zero)
  metrics  — 8888                      (always present; Prometheus scrape target)

Values consumed:
  .Values.service.{type,httpPort,grpcPort,sessionAffinity}
*/}}
{{- define "ev-common.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "ev-common.fullname" . }}
  labels:
    {{- include "ev-common.labels" . | nindent 4 }}
spec:
  type: {{ .Values.service.type }}
  {{- if and .Values.service.sessionAffinity (ne .Values.service.sessionAffinity "None") }}
  sessionAffinity: {{ .Values.service.sessionAffinity }}
  {{- end }}
  ports:
    - name:       http
      port:        {{ .Values.service.httpPort }}
      targetPort:  http
      protocol:    TCP
    {{- if and .Values.service.grpcPort (ne (int .Values.service.grpcPort) 0) }}
    - name:       grpc
      port:        {{ .Values.service.grpcPort }}
      targetPort:  grpc
      protocol:    TCP
    {{- end }}
    - name:       metrics
      port:        8888
      targetPort:  metrics
      protocol:    TCP
  selector:
    {{- include "ev-common.selectorLabels" . | nindent 4 }}
{{- end }}
