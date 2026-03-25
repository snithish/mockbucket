FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -trimpath \
    -o /mockbucketd \
    ./cmd/mockbucketd

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /mockbucketd /mockbucketd
COPY docker/config.yaml /etc/mockbucket/config.yaml

EXPOSE 9000

USER nonroot

ENTRYPOINT ["/mockbucketd"]
CMD ["--config", "/etc/mockbucket/config.yaml"]
