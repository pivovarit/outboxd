module github.com/pivovarit/outboxd/example

go 1.25.0

require (
	github.com/jackc/pgx/v5 v5.9.1
	github.com/pivovarit/outboxd v0.0.0
	github.com/rabbitmq/amqp091-go v1.10.0
)

require (
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pglogrepl v0.0.0-20251213150135-2e8d0df862c1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	golang.org/x/text v0.34.0 // indirect
)

replace github.com/pivovarit/outboxd => ../
