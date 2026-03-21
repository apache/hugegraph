# Deploy Hugegraph server with docker

> Note:
>
> 1. The docker image of hugegraph is a convenience release, not official distribution artifacts from ASF. You can find more details from [ASF Release Distribution Policy](https://infra.apache.org/release-distribution.html#dockerhub).
>
> 2. Recommend to use `release tag` (like `1.5.0`/`1.7.0`) for the stable version. Use `latest` tag to experience the newest functions in development.

## 1. Deploy

We can use docker to quickly start an inner HugeGraph server with RocksDB in the background.

1. Using docker run

   Use `docker run -itd --name=graph -p 8080:8080 hugegraph/hugegraph:1.3.0` to start hugegraph server.

2. Using docker compose

   Certainly we can only deploy server without other instance. Additionally, if we want to manage other HugeGraph-related instances with `server` in a single file, we can deploy HugeGraph-related instances via `docker-compose up -d`. The `docker-compose.yaml` is as below:

    ```yaml
    version: '3'
    services:
      graph:
        image: hugegraph/hugegraph:1.3.0
        ports:
          - 8080:8080
    ```

## 2. Create Sample Graph on Server Startup

If you want to **preload** some (test) data or graphs in container(by default), you can set the env `PRELOAD=ture`

If you want to customize the preloaded data, please mount the groovy scripts (not necessary).

1. Using docker run

   Use `docker run -itd --name=graph -p 8080:8080 -e PRELOAD=true -v /path/to/script:/hugegraph-server/scripts/example.groovy hugegraph/hugegraph:1.3.0`
   to start hugegraph server.

2. Using docker compose

   We can also use `docker-compose up -d` to quickly start. The `docker-compose.yaml` is below. [example.groovy](https://github.com/apache/hugegraph/blob/master/hugegraph-server/hugegraph-dist/src/assembly/static/scripts/example.groovy) is a pre-defined script. If needed, we can mount a new `example.groovy` to preload different data:

    ```yaml
    version: '3'
    services:
      graph:
        image: hugegraph/hugegraph:1.3.0
        environment:
          - PRELOAD=true
        volumes:
          - /path/to/script:/hugegraph-server/scripts/example.groovy
        ports:
          - 8080:8080
    ```

3. Using start-hugegraph.sh

   If you deploy HugeGraph server without docker, you can also pass arguments using `-p`, like this: `bin/start-hugegraph.sh -p true`.

## 3. Enable Authentication

1. Using docker run

   Use `docker run -itd --name=graph -p 8080:8080 -e AUTH=true -e PASSWORD=xxx hugegraph/hugegraph:1.3.0` to enable the authentication and set the password with `-e AUTH=true -e PASSWORD=xxx`.

2. Using docker compose

   Similarly, we can set the environment variables in the docker-compose.yaml:

    ```yaml
    version: '3'
    services:
      server:
        image: hugegraph/hugegraph:1.3.0
        container_name: graph
        ports:
          - 8080:8080
        environment:
          - AUTH=true
          - PASSWORD=xxx
    ```

## 4. Running Open-Telemetry-Collector

> CAUTION:
>
> The `docker-compose-trace.yaml` utilizes `Grafana` and `Grafana-Tempo`, both of them are licensed under [AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.en.html), you should be aware of and use them with caution. Currently, we mainly provide this template for everyone to **test**
>

1. Start Open-Telemetry-Collector

    ```bash
    docker compose -f hugegraph-server/hugegraph-dist/docker/example/docker-compose-trace.yaml -p hugegraph-trace up -d
    ```

2. Active Open-Telemetry-Agent

    ```bash
    ./start-hugegraph.sh -y true
    ```

3. Stop Open-Telemetry-Collector

    ```bash
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
docker compose -f docker-compose-3pd-3store-3server.yml up -d
```

See [docker/README.md](../../../../docker/README.md) for the full setup guide,
environment variable reference, and troubleshooting.
