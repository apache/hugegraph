# Deploy HugeGraph Server with Docker

> Note:
>
> 1. The HugeGraph Docker image is a convenience release, not an official ASF distribution artifact. See the [ASF Release Distribution Policy](https://infra.apache.org/release-distribution.html#dockerhub) for details.
>
> 2. Use release tags (for example, `1.7.0`) for stable deployments. Use `latest` only for development or testing.

## 1. Deploy

Use Docker to quickly start a standalone HugeGraph Server with RocksDB.

1. Using `docker run`

   Use `docker run -itd --name=graph -p 8080:8080 hugegraph/hugegraph:1.7.0` to start hugegraph server.

2. Using `docker compose`

   To deploy only the server, use `docker compose up -d`. The compose file is as follows:

    ```yaml
    version: '3'
    services:
      graph:
        image: hugegraph/hugegraph:1.7.0
        ports:
          - 8080:8080
    ```

## 2. Create Sample Graph on Server Startup

To preload sample data on startup, set `PRELOAD=true`.

To customize the preload, mount your own Groovy script.

1. Using `docker run`

   Use `docker run -itd --name=graph -p 8080:8080 -e PRELOAD=true -v /path/to/script:/hugegraph-server/scripts/example.groovy hugegraph/hugegraph:1.7.0`
   to start hugegraph server.

2. Using `docker compose`

   Use `docker compose up -d` to start quickly. The compose file is below. [example.groovy](https://github.com/apache/hugegraph/blob/master/hugegraph-server/hugegraph-dist/src/assembly/static/scripts/example.groovy) is a predefined script. Replace it with your own script to preload different data:

    ```yaml
    version: '3'
    services:
      graph:
        image: hugegraph/hugegraph:1.7.0
        environment:
          - PRELOAD=true
        volumes:
          - /path/to/script:/hugegraph-server/scripts/example.groovy
        ports:
          - 8080:8080
    ```

3. Using `start-hugegraph.sh`

   If you deploy HugeGraph Server without Docker, you can also pass `-p true` to `bin/start-hugegraph.sh`.

## 3. Enable Authentication

1. Using `docker run`

   Use `docker run -itd --name=graph -p 8080:8080 -e AUTH=true -e PASSWORD=xxx hugegraph/hugegraph:1.7.0` to enable authentication.

2. Using `docker compose`

   Set the environment variables in the compose file:

    ```yaml
    version: '3'
    services:
      server:
        image: hugegraph/hugegraph:1.7.0
        container_name: graph
        ports:
          - 8080:8080
        environment:
          - AUTH=true
          - PASSWORD=xxx
    ```

## 4. Run OpenTelemetry

> CAUTION:
>
> The `docker-compose-trace.yaml` uses Grafana and Grafana Tempo, both of which are licensed under [AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.en.html). Use this template for testing only.
>

1. Start the OpenTelemetry collector

    ```bash
    # Run from the repository root
    docker compose -f hugegraph-server/hugegraph-dist/docker/example/docker-compose-trace.yaml -p hugegraph-trace up -d
    ```

2. Enable the OpenTelemetry agent

    ```bash
    ./start-hugegraph.sh -y true
    ```

3. Stop the OpenTelemetry collector

    ```bash
    # Run from the repository root
    docker compose -f hugegraph-server/hugegraph-dist/docker/example/docker-compose-trace.yaml -p hugegraph-trace stop
    ```

4. References

    - [What is OpenTelemetry](https://opentelemetry.io/docs/what-is-opentelemetry/)

    - [Tempo in Grafana](https://grafana.com/docs/tempo/latest/getting-started/tempo-in-grafana/)

## 5. Distributed Cluster (PD + Store + Server)

For a full distributed HugeGraph cluster with PD, Store, and Server, use the
3-node compose file in the `docker/` directory at the repository root.

**Prerequisites**: Allocate at least **12 GB** memory to Docker Desktop
(Settings → Resources → Memory). The cluster runs 9 JVM processes.

```bash
cd docker
HUGEGRAPH_VERSION=1.7.0 docker compose -f docker-compose-3pd-3store-3server.yml up -d
```

See [docker/README.md](../../../docker/README.md) for the full setup guide,
environment variable reference, and troubleshooting.

## 6. Process Supervision & Health Checks

All four HugeGraph Docker images (PD, Store, Server, Server-hstore) include
native `HEALTHCHECK` instructions. `docker ps` shows real health status:

| Image | Health endpoint |
|---|---|
| `hugegraph/hugegraph` | `GET /versions` on port 8080 |
| `hugegraph/hugegraph-hstore` | `GET /versions` on port 8080 |
| `hugegraph/hugegraph-pd` | `GET /v1/health` on port 8620 |
| `hugegraph/hugegraph-store` | `GET /v1/health` on port 8520 |

The entrypoints supervise the Java process directly — when Java exits, the container exits. If started with a restart policy (the provided compose files use `restart: unless-stopped`), Docker will bring it back automatically. The old cron-based monitor (`-m true`) is for VM/bare-metal deployments only and is not used in Docker images.

## 7. Server Startup Timeout

The entrypoint gives the Server a fixed budget to answer on its REST port and
ends the container when the budget runs out. It is 120 seconds by default. Set
`HG_SERVER_STARTUP_TIMEOUT_S` to a whole number of seconds between 1 and 86400
to change it:

```bash
docker run -itd --name=graph -p 8080:8080 -e HG_SERVER_STARTUP_TIMEOUT_S=450 hugegraph/hugegraph:1.7.0
```

The Compose topologies under `docker/` pass the variable through to every
Server with an unset-only default, so `HG_SERVER_STARTUP_TIMEOUT_S=300 docker
compose up -d` raises the budget without editing a Compose file, while leaving
it unset renders the same 120.

Raise it on slow or contended hosts, and wherever an orchestrator already owns
the startup budget through a probe of its own: a startup probe cannot extend a
container that has already ended the JVM it was waiting for. Anything outside
the accepted range, an empty value included, stops the container at startup
instead of silently falling back to the default.

Raising this budget does not move the health check described in section 6,
which runs on a clock of its own. The images set `--interval=15s
--start-period=90s --retries=3`, so a container given a longer startup budget
is reported `unhealthy` around 135 seconds while the entrypoint is still
legitimately waiting; raise it with `--health-start-period` on `docker run`.
The Compose files replace those values with their own (`start_period: 60s`,
`interval: 10s`, `retries: 30`, so roughly 360 seconds), and anything gated on
`depends_on: condition: service_healthy`, Hubble included, waits on that budget
rather than on this variable. Move the two together.
