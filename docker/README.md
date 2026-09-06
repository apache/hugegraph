# HugeGraph Docker Compose

## Users

### Choose a topology

Run commands in this directory:

```bash
cd docker
```

| Topology | Compose file | Services | When to use it |
| --- | --- | --- | --- |
| Standalone | `docker-compose.yml` | 1 RocksDB Server + 1 Hubble | Default; start here |
| Minimal HStore | `docker-compose-hstore.yml` | 1 PD + 1 Store + 1 Server + 1 Hubble | Distributed local development |
| HA | `docker-compose-3pd-3store-3server.yml` | 3 PD + 3 Store + 3 Server + 1 Hubble | Reference and evaluation |

Standalone uses `hugegraph/hugegraph:${HUGEGRAPH_VERSION:-latest}`. The HStore
topologies use the matching `hugegraph/pd`, `hugegraph/store`, and
`hugegraph/server` tags. Hubble is selected independently with
`${HUBBLE_IMAGE:-hugegraph/hubble:latest}`.

### Create the authentication environment

Create `.env` once. Replace `replace-with-your-password` with an administrator
password that you choose; the command generates and persists a random 32-byte
JWT secret. For this simple single-quoted format, do not use a password that
contains a single quote or newline.

```bash
(
  set -eu
  command -v openssl >/dev/null
  jwt_secret="$(openssl rand -hex 32)"
  test "${#jwt_secret}" -eq 64
  umask 077
  test ! -e .env || {
    echo ".env already exists; edit it instead of overwriting it" >&2
    exit 1
  }
  printf "HUGEGRAPH_ADMIN_PASSWORD='%s'\nHUGEGRAPH_AUTH_TOKEN_SECRET='%s'\n" \
    'replace-with-your-password' "${jwt_secret}" > .env
)
```

Do not commit `.env`. Keeping the same JWT secret preserves authentication
tokens when containers are recreated. For authenticated topologies with
multiple Server replicas, all replicas receive this same secret. The HA
topology fails fast if authentication is enabled without this shared secret.

A non-empty `HUGEGRAPH_ADMIN_PASSWORD` enables Server authentication, and
Hubble detects that mode automatically. Omitting the variable or setting it to
an empty value disables authentication. Auth-off is only suitable for a
trusted local environment; never expose it to a public or untrusted network.
Hubble listens on host loopback by default. Set `HUBBLE_PUBLISH_HOST` only
behind an HTTPS reverse proxy and trusted network controls.

`HUGEGRAPH_ADMIN_PASSWORD` initializes the built-in `admin` account on its
first authenticated startup. Changing `.env` does not rotate an existing
administrator password; use the HugeGraph user API for credential changes.

For the verification commands below, set the password in your current shell:

```bash
ADMIN_PASSWORD='the-same-password-used-in-.env'
```

### Standalone

This is the recommended quickstart.

Start:

```bash
docker compose -f docker-compose.yml up -d --wait
```

Status:

```bash
docker compose -f docker-compose.yml ps
```

Verify Server readiness, authentication, and Hubble:

```bash
curl -fsS http://localhost:8080/versions
test "$(curl -sS -o /dev/null -w '%{http_code}' \
  http://localhost:8080/graphspaces/DEFAULT/graphs)" = 401
test "$(curl -sS -u "admin:${ADMIN_PASSWORD}" -o /dev/null -w '%{http_code}' \
  http://localhost:8080/graphspaces/DEFAULT/graphs)" = 200
curl -fsS http://localhost:8088/about
```

Open `http://localhost:8088` and sign in as `admin` with the password from
`.env`.

Stop containers while keeping them:

```bash
docker compose -f docker-compose.yml stop
```

Remove containers and the network while keeping data:

```bash
docker compose -f docker-compose.yml down
```

Delete containers, the network, and all topology data:

```bash
docker compose -f docker-compose.yml down -v
```

### Minimal HStore

Start:

```bash
docker compose -f docker-compose-hstore.yml up -d --wait
```

Status:

```bash
docker compose -f docker-compose-hstore.yml ps
```

Verify PD, Store, Server authentication, and Hubble:

```bash
curl -fsS http://localhost:8620/v1/health
curl -fsS http://localhost:8520/v1/health
curl -fsS http://localhost:8080/versions
test "$(curl -sS -o /dev/null -w '%{http_code}' \
  http://localhost:8080/graphspaces/DEFAULT/graphs)" = 401
test "$(curl -sS -u "admin:${ADMIN_PASSWORD}" -o /dev/null -w '%{http_code}' \
  http://localhost:8080/graphspaces/DEFAULT/graphs)" = 200
curl -fsS http://localhost:8088/about
```

Open `http://localhost:8088` and sign in as `admin` with the password from
`.env`.

Stop containers while keeping them:

```bash
docker compose -f docker-compose-hstore.yml stop
```

Remove containers and the network while keeping data:

```bash
docker compose -f docker-compose-hstore.yml down
```

Delete containers, the network, and all topology data:

```bash
docker compose -f docker-compose-hstore.yml down -v
```

### HA reference

The HA topology is resource-intensive. Running it locally is not required on
resource-constrained machines, but its Compose configuration must always render
successfully. This PR validates HA by rendering and static review only; it does
not start HA locally or in default CI.

Start:

```bash
docker compose -f docker-compose-3pd-3store-3server.yml up -d --wait
```

Status:

```bash
docker compose -f docker-compose-3pd-3store-3server.yml ps
```

Verify all published PD, Store, and Server endpoints, Server authentication,
and Hubble:

```bash
for port in 8620 8621 8622; do
  curl -fsS "http://localhost:${port}/v1/health"
done
for port in 8520 8521 8522; do
  curl -fsS "http://localhost:${port}/v1/health"
done
for port in 8080 8081 8082; do
  curl -fsS "http://localhost:${port}/versions"
  test "$(curl -sS -o /dev/null -w '%{http_code}' \
    "http://localhost:${port}/graphspaces/DEFAULT/graphs")" = 401
  test "$(curl -sS -u "admin:${ADMIN_PASSWORD}" -o /dev/null \
    -w '%{http_code}' \
    "http://localhost:${port}/graphspaces/DEFAULT/graphs")" = 200
done
curl -fsS http://localhost:8088/about
```

PD answers two unauthenticated probe endpoints. `/v1/health` is liveness only:
it returns `200` as soon as the REST listener is up, even when the PD has no
raft leader. `/v1/ready` returns `200` only while the PD sees a raft leader,
and `503` otherwise. A single PD elects itself; three PDs become ready once
two of them can talk to each other.

The healthchecks in these files still gate on `/v1/health`, because
`/v1/ready` ships from the next release onwards while the files run published
images. Two things to know before pointing them at readiness:

- Match on the body, not the status code. As of 1.7.0 PD answers `200` with
  `{"status":-1,"error":"Unauthorized!"}` on every path its auth interceptor
  does not exclude, a path that does not exist included, so a status-only
  probe reads a PD too old to have `/v1/ready` as ready. The body match holds
  whichever status a refusal carries. Gate with
  `curl -fsS http://localhost:8620/v1/ready | grep -q '"ready":true'` instead.
- Pin `HUGEGRAPH_VERSION` to a release that carries the endpoint, or build the
  images from source with `docker-compose.dev.yml`.

The `HEALTHCHECK` baked into `hugegraph-pd/Dockerfile` is `/v1/health` as well.
Both compose files override it, so it governs `docker run` and anything else
inheriting the image probe, and those keep reading a PD without a quorum as
healthy.

Open `http://localhost:8088` and sign in as `admin` with the password from
`.env`.

Stop containers while keeping them:

```bash
docker compose -f docker-compose-3pd-3store-3server.yml stop
```

Remove containers and the network while keeping data:

```bash
docker compose -f docker-compose-3pd-3store-3server.yml down
```

Delete containers, the network, and all topology data:

```bash
docker compose -f docker-compose-3pd-3store-3server.yml down -v
```

### Select image versions

Set a HugeGraph release for Server, PD, and Store without changing Hubble:

```bash
HUGEGRAPH_VERSION=1.7.0 \
docker compose -f docker-compose-hstore.yml up -d
```

Select Hubble independently:

```bash
HUBBLE_IMAGE=hugegraph/hubble:latest \
docker compose -f docker-compose.yml up -d
```

The Hubble `latest` image is expected to work with HugeGraph Server 1.7 and
Server `latest`; compatibility with versions older than 1.7 is not promised.
Pin immutable image references when reproducibility is required.

### Data persistence

Each topology creates its own normal Compose network and named volumes. No
network or volume needs to be created in advance.

Standalone stores RocksDB data at `/hugegraph-server/rocksdb-data`. The HStore
topologies keep PD and Store data in topology-local volumes. Hubble uses
`jdbc:h2:file:/hubble/data/hubble;DB_CLOSE_ON_EXIT=FALSE` and stores uploaded
files under `/hubble/data/upload-files`.

`docker compose down` keeps named-volume data. `docker compose down -v`
intentionally deletes it.

## Developers

### Images and Compose files

| Image | Build file |
| --- | --- |
| `hugegraph/hugegraph` (standalone RocksDB Server) | `hugegraph-server/Dockerfile` |
| `hugegraph/server` (HStore Server) | `hugegraph-server/Dockerfile-hstore` |
| `hugegraph/pd` | `hugegraph-pd/Dockerfile` |
| `hugegraph/store` | `hugegraph-store/Dockerfile` |

Hubble is built from the separate HugeGraph Toolchain repository and is
selected here with `HUBBLE_IMAGE`.

The Compose mapping is intentionally small:

- `docker-compose.yml` is the standalone user default.
- `docker-compose-hstore.yml` is the minimal 1 PD + 1 Store + 1 Server base.
- `docker-compose-3pd-3store-3server.yml` is the HA reference.
- `docker-compose.dev.yml` is a thin source-build override for the minimal
  HStore topology. It does not duplicate runtime services, networks, volumes,
  health checks, or Hubble.

Build and start the minimal topology from local source:

```bash
docker compose \
  -f docker-compose-hstore.yml \
  -f docker-compose.dev.yml \
  up -d --build --wait
```

Use both files for every later lifecycle command, for example:

```bash
docker compose \
  -f docker-compose-hstore.yml \
  -f docker-compose.dev.yml \
  down
```

The development overlay builds `hugegraph/pd:dev`, `hugegraph/store:dev`, and
`hugegraph/server:dev`. To reuse those local images and a locally built Hubble
without pulling replacements:

```bash
HUGEGRAPH_VERSION=dev \
HUGEGRAPH_PULL_POLICY=never \
HUBBLE_IMAGE=local/hugegraph-hubble:test \
HUBBLE_PULL_POLICY=never \
docker compose -f docker-compose-hstore.yml up -d --wait
```

### Hubble configuration

The three small files under `conf/hubble/` contain only topology-specific
discovery settings and container paths:

- `conf/hubble/standalone.properties` uses direct Server mode.
- `conf/hubble/hstore.properties` uses one PD and one Store REST target.
- `conf/hubble/hstore-ha.properties` uses all three PD peers and all three
  allowed Store REST targets.

Hubble detects Server authentication through the Server API. Do not add an
`auth.enabled` property or duplicate auth-on/auth-off configurations.

### Render and smoke checks

Render every topology with auth-on inputs before submitting a change:

```bash
bash test-compose.sh render
```

The HA render is mandatory even when local resources are insufficient to start
its ten containers.

Run focused auth-on smoke checks for standalone and minimal HStore with the
corresponding `up -d --wait`, status, authentication, Hubble `/about`, and
`down -v` commands from the Users section:

```bash
bash test-compose.sh smoke
```

Run the required local auth-off checks separately:

```bash
bash test-compose.sh smoke-auth-off
```

The auth-off mode is intentionally excluded from the default CI matrix and must
remain on a trusted local machine. Both smoke modes remove only the isolated
Compose projects and volumes that they create.
