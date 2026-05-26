FROM golang:1.25.5-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o now-search .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/now-search .

EXPOSE 8080

CMD ["./now-search"]