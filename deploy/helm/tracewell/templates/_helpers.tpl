{{/* Labels shared by everything in the chart. */}}
{{- define "tracewell.labels" -}}
app.kubernetes.io/part-of: tracewell
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
