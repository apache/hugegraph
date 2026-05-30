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
