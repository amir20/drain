FROM --platform=$BUILDPLATFORM golang:1.23.0-alpine AS builder

RUN apk add --no-cache ca-certificates && mkdir /build

WORKDIR /build

COPY go.* ./
RUN go mod download

COPY internal ./internal
COPY main.go ./

ARG TAG=dev
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=$TAG" -o drain

RUN mkdir /data

FROM scratch

ENV PATH /bin
COPY --from=builder /data /data
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /build/drain /drain

EXPOSE 4000

ENTRYPOINT ["/drain"]
