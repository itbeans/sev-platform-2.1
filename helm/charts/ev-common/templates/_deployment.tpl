{{/*
Standard Deployment for an ev-server microservice.

Values consumed:
  .Values.image.{repository,tag,pullPolicy}
  .Values.replicaCount
  .Values.autoscaling.enabled
  .Values.service.{httpPort,grpcPort}
  .Values.terminationGracePeriodSeconds   (default 60)
  .Values.resources.{requests,limits}
  .Values.env                             (map[string]string — non-secret env vars)
  .Values.envFromSecrets                  (list of Secret names)

Fixed behaviour:
  - Prometheus annotations on the pod (port 8888, path /metrics)
  - security context: runAsNonRoot, allowPrivilegeEscalation=false, drop ALL caps,
    readOnlyRootFilesystem=true
  - emptyDir at /tmp so the JVM can write temp files
  - preStop sleep 5 s to drain in-flight requests before SIGTERM
  - liveness / readiness probes on GET /health (httpPort)
*/}}
{{- define "ev-common.deployment" -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "ev-common.fullname" . }}
  labels:
    {{- include "ev-common.labels" . | nindent 4 }}
spec:
  {{- if not .Values.autoscaling.enabled }}
  replicas: {{ .Values.replicaCount }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "ev-common.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port:   "8888"
        prometheus.io/path:   "/metrics"
      labels:
        {{- include "ev-common.selectorLabels" . | nindent 8 }}
    spec:
      terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds | default 60 }}
      securityContext:
        runAsNonRoot: true
        runAsUser:    1000
        fsGroup:      1000
      volumes:
        - name: tmp-dir
          emptyDir: {}
      containers:
        - name: {{ .Chart.Name }}
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          securityContext:
            readOnlyRootFilesystem:   true
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: tmp-dir
              mountPath: /tmp
          ports:
            - name: http
              containerPort: {{ .Values.service.httpPort }}
              protocol: TCP
            {{- if and .Values.service.grpcPort (ne (int .Values.service.grpcPort) 0) }}
            - name: grpc
              containerPort: {{ .Values.service.grpcPort }}
              protocol: TCP
            {{- end }}
            - name: metrics
              containerPort: 8888
              protocol: TCP
          lifecycle:
            preStop:
              exec:
                command: ["/bin/sh", "-c", "sleep 5"]
          env:
            {{- range $key, $val := .Values.env }}
            - name: {{ $key }}
              value: {{ $val | quote }}
            {{- end }}
          {{- if .Values.envFromSecrets }}
          envFrom:
            {{- range .Values.envFromSecrets }}
            - secretRef:
                name: {{ . }}
            {{- end }}
          {{- end }}
          livenessProbe:
            httpGet:
              path: /health
              port: http
            initialDelaySeconds: 30
            periodSeconds:       15
            failureThreshold:    3
          readinessProbe:
            httpGet:
              path: /health
              port: http
            initialDelaySeconds: 10
            periodSeconds:       10
            failureThreshold:    3
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
{{- end }}
