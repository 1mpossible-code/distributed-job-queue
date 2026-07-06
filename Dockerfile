FROM golang:1.23-alpine AS build
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /out/worker ./cmd/worker
RUN go build -o /out/producer ./cmd/producer
RUN go build -o /out/bench ./cmd/bench

FROM alpine:latest
RUN apk add --no-cache bash redis ttyd
COPY --from=build /out/worker /usr/local/bin/worker
COPY --from=build /out/producer /usr/local/bin/producer
COPY --from=build /out/bench /usr/local/bin/bench
COPY demo-shell.sh /usr/local/bin/demo-shell.sh
RUN chmod +x /usr/local/bin/demo-shell.sh

CMD ["ttyd", "-W", "-p", "7681", "demo-shell"]
