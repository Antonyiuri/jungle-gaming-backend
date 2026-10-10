FROM golang:1.27.2 AS source
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM source AS build
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

# Race-instrumented build for the end-to-end suite (compose.race.yaml). It needs cgo and glibc,
# so it has its own Debian runtime; the default (last) target below stays the small Alpine image.
FROM source AS build-race
# -p 2 keeps peak memory low: the cgo/race build of all packages at once exhausted a 3.5 GB Docker VM.
RUN CGO_ENABLED=1 go build -p 2 -race -trimpath -o /out/server ./cmd/server

FROM debian:bookworm-slim AS runtime-race
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build-race /out/server /app/server
COPY deploy/app-entrypoint.sh /app/entrypoint.sh
RUN chmod 755 /app/entrypoint.sh
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/bin/sh", "/app/entrypoint.sh"]

FROM alpine:3.22.2
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/server /app/server
COPY deploy/app-entrypoint.sh /app/entrypoint.sh
RUN chmod 755 /app/entrypoint.sh
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/bin/sh", "/app/entrypoint.sh"]
