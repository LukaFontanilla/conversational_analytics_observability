# Use the official Golang image to create a build artifact.
# This is based on Debian and sets the GOPATH to /go.
# https://hub.docker.com/_/golang
FROM golang:1.21-bullseye as builder

# Create and change to the app directory.
WORKDIR /app

# Retrieve application dependencies using go.mod and go.sum.
# Copy only these files to prevent unnecessary rebuilds.
COPY go.* ./
RUN go mod download

# Copy local code to the container image.
COPY . ./

# Build the binary.
# -mod=readonly ensures go.mod is not modified.
RUN go build -mod=readonly -v -o dailyCron

# Use the official Debian slim image for a lean production container.
# https://hub.docker.com/_/debian
FROM debian:bullseye-slim
RUN set -x && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy the binary to the production image from the builder stage.
COPY --from=builder /app/dailyCron /app/dailyCron

# Run the web service on container startup.
CMD ["/app/dailyCron"]
