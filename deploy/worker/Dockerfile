# syntax=docker/dockerfile:1.7
ARG GOLANG_IMAGE=golang:1.27.4-alpine
FROM --platform=${BUILDPLATFORM} ${GOLANG_IMAGE} AS builder
ARG TARGETOS
ARG TARGETARCH
RUN apk add --no-cache ca-certificates
WORKDIR /src/backend
COPY backend/ ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/nexus-worker ./cmd/worker

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/nexus-worker /nexus-worker
USER 1000:1000
ENTRYPOINT ["/nexus-worker"]
