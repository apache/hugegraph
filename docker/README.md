# HugeGraph Docker Deployment

This directory contains Docker Compose files for running HugeGraph:

| File | Description |
|------|-------------|
| `docker-compose.yml` | Single-node cluster using pre-built images from Docker Hub |
| `docker-compose.dev.yml` | Single-node cluster built from source (for developers) |
| `docker-compose-3pd-3store-3server.yml` | 3-node distributed cluster (PD + Store + Server) |

## Prerequisites

- **Docker Engine** 20.10+ (or Docker Desktop 4.x+)
- **Docker Compose** v2 (included in Docker Desktop)
- **Memory**: Allocate at least **12 GB** to Docker Desktop (Settings → Resources → Memory). The 3-node cluster runs 9 JVM processes (3 PD + 3 Store + 3 Server) which are memory-intensive. Insufficient memory causes OOM kills that appear as silent Raft failures.

> [!IMPORTANT]
> The 12 GB minimum is for Docker Desktop. On Linux with native Docker, ensure the host has at least 12 GB of free memory.
---

## Single-Node Setup

Two compose files are available for running a single-node cluster (1 PD + 1 Store + 1 Server):

### Option A: Quick Start (pre-built images)

Uses pre-built images from Docker Hub. Best for **end users** who want to run HugeGraph quickly.

```bash
cd docker
HUGEGRAPH_VERSION=1.7.0 docker compose up -d
```

- Images: `hugegraph/pd:1.7.0`, `hugegraph/store:1.7.0`, `hugegraph/server:1.7.0`
- `pull_policy: always` — always pulls the specified image tag

> **Note**: Use release tags (e.g., `1.7.0`) for stable deployments. The `latest` tag is intended for testing or development only.
- PD healthcheck endpoint: `/v1/health`
- Single PD, single Store (`HG_PD_INITIAL_STORE_LIST: store:8500`), single Server
- Server healthcheck endpoint: `/versions`

### Option B: Development Build (build from source)

Builds images locally from source Dockerfiles. Best for **developers** who want to test local changes.

```bash
cd docker
docker compose -f docker-compose.dev.yml up -d
```

- Images: built from source via `build: context: ..` with Dockerfiles
- No `pull_policy` — builds locally, doesn't pull
- Entrypoint scripts are baked into the built image (no volume mounts)
- PD healthcheck endpoint: `/v1/health`
- Otherwise identical env vars and structure to the quickstart file

### Key Differences

| | `docker-compose.yml` (quickstart) | `docker-compose.dev.yml` (dev build) |
|---|---|---|
| **Images** | Pull from Docker Hub | Build from source |
| **Who it's for** | End users | Developers |
| **pull_policy** | `always` | not set (build) |

**Verify** (both options):
```bash
curl http://localhost:8080/versions
```

---

## 3-Node Cluster Quickstart

```bash
cd docker
HUGEGRAPH_VERSION=1.7.0 docker compose -f docker-compose-3pd-3store-3server.yml up -d

# To stop and remove all data volumes (clean restart)
docker compose -f docker-compose-3pd-3store-3server.yml down -v
```

**Startup ordering** is enforced via `depends_on` with `condition: service_healthy`:

1. **PD nodes** start first and must pass healthchecks (`/v1/health`)
2. **Store nodes** start after all PD nodes are healthy
3. **Server nodes** start after all Store nodes are healthy

This ensures PD and Store are healthy before the server starts. The server entrypoint still performs a best-effort partition wait after launch, so partition assignment may take a little longer.

**Verify the cluster is healthy**:

```bash
# Check PD health
curl http://localhost:8620/v1/health

# Check Store health
curl http://localhost:8520/v1/health

# Check Server (Graph API)
curl http://localhost:8080/versions

# List registered stores via PD
curl http://localhost:8620/v1/stores

# List partitions
curl http://localhost:8620/v1/partitions
```

---

## Environment Variable Reference

Configuration is injected via environment variables. The old `docker/configs/application-pd*.yml` and `docker/configs/application-store*.yml` files are no longer used.

### PD Environment Variables

| Variable | Required | Default | Maps To (`application.yml`) | Description |
|----------|----------|---------|-----------------------------|-------------|
| `HG_PD_GRPC_HOST` | Yes | — | `grpc.host` | This node's hostname/IP for gRPC |
| `HG_PD_RAFT_ADDRESS` | Yes | — | `raft.address` | This node's Raft address (e.g. `pd0:8610`) |
| `HG_PD_RAFT_PEERS_LIST` | Yes | — | `raft.peers-list` | All PD peers (e.g. `pd0:8610,pd1:8610,pd2:8610`) |
| `HG_PD_INITIAL_STORE_LIST` | Yes | — | `pd.initial-store-list` | Expected stores (e.g. `store0:8500,store1:8500,store2:8500`) |
| `HG_PD_GRPC_PORT` | No | `8686` | `grpc.port` | gRPC server port |
| `HG_PD_REST_PORT` | No | `8620` | `server.port` | REST API port |
| `HG_PD_DATA_PATH` | No | `/hugegraph-pd/pd_data` | `pd.data-path` | Metadata storage path |
| `HG_PD_INITIAL_STORE_COUNT` | No | `1` | `pd.initial-store-count` | Min stores for cluster availability |

**Deprecated aliases** (still work but log a warning):

| Deprecated | Use Instead |
|------------|-------------|
| `GRPC_HOST` | `HG_PD_GRPC_HOST` |
| `RAFT_ADDRESS` | `HG_PD_RAFT_ADDRESS` |
| `RAFT_PEERS` | `HG_PD_RAFT_PEERS_LIST` |
| `PD_INITIAL_STORE_LIST` | `HG_PD_INITIAL_STORE_LIST` |

### Store Environment Variables

| Variable | Required | Default | Maps To (`application.yml`) | Description |
|----------|----------|---------|-----------------------------|-------------|
| `HG_STORE_PD_ADDRESS` | Yes | — | `pdserver.address` | PD gRPC addresses (e.g. `pd0:8686,pd1:8686,pd2:8686`) |
| `HG_STORE_GRPC_HOST` | Yes | — | `grpc.host` | This node's hostname (e.g. `store0`) |
| `HG_STORE_RAFT_ADDRESS` | Yes | — | `raft.address` | This node's Raft address (e.g. `store0:8510`) |
| `HG_STORE_GRPC_PORT` | No | `8500` | `grpc.port` | gRPC server port |
| `HG_STORE_REST_PORT` | No | `8520` | `server.port` | REST API port |
| `HG_STORE_DATA_PATH` | No | `/hugegraph-store/storage` | `app.data-path` | Data storage path |

**Deprecated aliases** (still work but log a warning):

| Deprecated | Use Instead |
|------------|-------------|
| `PD_ADDRESS` | `HG_STORE_PD_ADDRESS` |
| `GRPC_HOST` | `HG_STORE_GRPC_HOST` |
| `RAFT_ADDRESS` | `HG_STORE_RAFT_ADDRESS` |

### Server Environment Variables

| Variable | Required | Default | Maps To | Description |
|----------|----------|---------|-----------------------------|-------------|
| `HG_SERVER_BACKEND` | Yes | — | `backend` in `hugegraph.properties` | Storage backend (e.g. `hstore`) |
| `HG_SERVER_PD_PEERS` | Yes | — | `pd.peers` | PD cluster addresses (e.g. `pd0:8686,pd1:8686,pd2:8686`) |
| `STORE_REST` | No | — | Used by `wait-partition.sh` | Store REST endpoint for partition verification (e.g. `store0:8520`) |
| `PASSWORD` | No | — | Enables auth mode | Optional authentication password |

**Deprecated aliases** (still work but log a warning):

| Deprecated | Use Instead |
|------------|-------------|
| `BACKEND` | `HG_SERVER_BACKEND` |
| `PD_PEERS` | `HG_SERVER_PD_PEERS` |

---

## Port Reference

The table below reflects the published host ports in `docker-compose-3pd-3store-3server.yml`.
The single-node compose file (`docker-compose.yml`) only publishes the REST/API ports (`8620`, `8520`, `8080`) by default.

| Service | Container Port | Host Port | Protocol | Purpose |
|---------|---------------|-----------|----------|---------|
| pd0 | 8620 | 8620 | HTTP | REST API |
| pd0 | 8686 | 8686 | gRPC | PD gRPC |
| pd0 | 8610 | — | TCP | Raft (internal only) |
| pd1 | 8620 | 8621 | HTTP | REST API |
| pd1 | 8686 | 8687 | gRPC | PD gRPC |
| pd2 | 8620 | 8622 | HTTP | REST API |
| pd2 | 8686 | 8688 | gRPC | PD gRPC |
| store0 | 8500 | 8500 | gRPC | Store gRPC |
| store0 | 8510 | 8510 | TCP | Raft |
| store0 | 8520 | 8520 | HTTP | REST API |
| store1 | 8500 | 8501 | gRPC | Store gRPC |
| store1 | 8510 | 8511 | TCP | Raft |
| store1 | 8520 | 8521 | HTTP | REST API |
| store2 | 8500 | 8502 | gRPC | Store gRPC |
| store2 | 8510 | 8512 | TCP | Raft |
| store2 | 8520 | 8522 | HTTP | REST API |
| server0 | 8080 | 8080 | HTTP | Graph API |
| server1 | 8080 | 8081 | HTTP | Graph API |
| server2 | 8080 | 8082 | HTTP | Graph API |

---

## Healthcheck Endpoints

| Service | Endpoint | Expected |
|---------|----------|----------|
| PD | `GET /v1/health` | `200 OK` |
| Store | `GET /v1/health` | `200 OK` |
| Server | `GET /versions` | `200 OK` with version JSON |

---

## Troubleshooting

### Containers Exiting or Restarting (OOM Kills)

**Symptom**: Containers exit with code 137, or restart loops. Raft logs show election timeouts.

**Cause**: Docker Desktop does not have enough memory. The 9 JVM processes require at least 12 GB.

**Fix**: Docker Desktop → Settings → Resources → Memory → set to **12 GB** or higher. Restart Docker Desktop.

```bash
# Check if containers were OOM killed
docker inspect hg-pd0 | grep -i oom
docker stats --no-stream
```

### Raft Leader Election Failure

**Symptom**: PD logs show repeated `Leader election timeout`. Store nodes cannot register.

**Cause**: PD nodes cannot reach each other on the Raft port (8610), or `HG_PD_RAFT_PEERS_LIST` is misconfigured.

**Fix**:
1. Verify all PD containers are running: `docker compose -f docker-compose-3pd-3store-3server.yml ps`
2. Check PD logs: `docker logs hg-pd0`
3. Verify network connectivity: `docker exec hg-pd0 ping pd1`
4. Ensure `HG_PD_RAFT_PEERS_LIST` is identical on all PD nodes

### Partition Assignment Not Completing

**Symptom**: Server starts but graph operations fail. Store logs show `partition not found`.

**Cause**: PD has not finished assigning partitions to stores, or stores did not register successfully.

**Fix**:
1. Check registered stores: `curl http://localhost:8620/v1/stores`
2. Check partition status: `curl http://localhost:8620/v1/partitions`
3. Wait for partition assignment (can take 1–3 minutes after all stores register)
4. Check server logs for the `wait-partition.sh` script output: `docker logs hg-server0`

### Connection Refused Errors

**Symptom**: Stores cannot connect to PD, or Server cannot connect to Store.

**Cause**: Services are using `127.0.0.1` instead of container hostnames, or the `hg-net` bridge network is misconfigured.

**Fix**: Ensure all `HG_*` env vars use container hostnames (`pd0`, `store0`, etc.), not `127.0.0.1` or `localhost`.
