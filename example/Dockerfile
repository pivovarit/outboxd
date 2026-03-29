FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod go.sum *.go ./

COPY example/go.mod example/go.sum ./example/
WORKDIR /src/example
RUN go mod download

COPY example/producer ./producer/
COPY example/relay ./relay/
COPY example/consumer ./consumer/
COPY example/multiple-relays ./multiple-relays/

RUN CGO_ENABLED=0 go build -o /bin/producer ./producer
RUN CGO_ENABLED=0 go build -o /bin/relay ./relay
RUN CGO_ENABLED=0 go build -o /bin/consumer ./consumer
RUN CGO_ENABLED=0 go build -o /bin/multiple-relays ./multiple-relays

FROM alpine:3.21 AS producer
COPY --from=build /bin/producer /usr/local/bin/producer
CMD ["producer"]

FROM alpine:3.21 AS relay
COPY --from=build /bin/relay /usr/local/bin/relay
CMD ["relay"]

FROM alpine:3.21 AS consumer
COPY --from=build /bin/consumer /usr/local/bin/consumer
CMD ["consumer"]

FROM alpine:3.21 AS multiple-relays
COPY --from=build /bin/multiple-relays /usr/local/bin/relay
CMD ["relay"]
