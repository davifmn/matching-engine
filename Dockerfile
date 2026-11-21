# syntax=docker/dockerfile:1

# Build on the runner's own platform and cross-compile: no emulation for arm64 images.
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
WORKDIR /src
COPY go.mod ./
COPY . .
ARG TARGETOS TARGETARCH VERSION=dev
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/matchd ./cmd/matchd \
 && mkdir -p /out/data

# Static binary on distroless: no shell, no package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/matchd /usr/local/bin/matchd
COPY config/instruments.json /etc/matchd/instruments.json
COPY --from=build --chown=65532:65532 /out/data /var/lib/matchd
ENV MATCHD_ADDR=:8080 \
    MATCHD_DATA=/var/lib/matchd \
    MATCHD_INSTRUMENTS=/etc/matchd/instruments.json
EXPOSE 8080
VOLUME ["/var/lib/matchd"]
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/matchd"]
