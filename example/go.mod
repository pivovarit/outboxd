module github.com/pivovarit/outboxd/example

go 1.26.0

require (
	github.com/jackc/pgx/v5 v5.9.2
	github.com/pivovarit/outboxd v0.0.0
	github.com/pivovarit/outboxd/middleware/otel v0.0.0-20260425065827-2fb36505d1eb
	github.com/rabbitmq/amqp091-go v1.10.0
	go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.43.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.43.0
	go.opentelemetry.io/otel/sdk v1.43.0
	go.opentelemetry.io/otel/sdk/metric v1.43.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pglogrepl v0.0.0-20251213150135-2e8d0df862c1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
)

replace (
	github.com/pivovarit/outboxd => ../
	github.com/pivovarit/outboxd/middleware/otel => ../middleware/otel
)
