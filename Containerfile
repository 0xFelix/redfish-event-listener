# Build stage
FROM docker.io/library/golang:1.24 AS builder

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# Copy the go source
COPY main.go main.go
COPY pkg/ pkg/
COPY vendor/ vendor/

# Build the application
RUN CGO_ENABLED=0 go build -o redfish-event-listener main.go

# Use distroless as minimal base image to package the sbetest binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/redfish-event-listener .
USER 65532:65532

ENTRYPOINT ["/redfish-event-listener"]
