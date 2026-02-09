FROM --platform=$BUILDPLATFORM golang:1.25.7-alpine AS base

RUN apk add --no-cache ca-certificates && mkdir /build

WORKDIR /build

COPY go.* ./
RUN go mod download

FROM base AS dev
RUN go install github.com/air-verse/air@latest
CMD ["air"]

# Build stage
FROM base AS builder
COPY . .
COPY internal ./internal
COPY main.go ./

RUN mkdir /data

ARG TAG=dev
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=$TAG" -o drain

FROM scratch

ENV PATH=/bin
COPY --from=builder /data /data
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /build/drain /drain

EXPOSE 4000

ENTRYPOINT ["/drain"]
