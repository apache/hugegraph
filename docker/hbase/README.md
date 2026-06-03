# HugeGraph + HBase Backend

This guide covers running HugeGraph with HBase backend.

> All commands below run from the repository root (this project folder).

Use this once at the start of your terminal session:

```bash
ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT_DIR"
```

---

## Quick Start Paths (Choose One)

<details>
<summary><b>Option 1: Standalone HugeGraph (using start-hugegraph.sh)</b></summary>

Prerequisite: build local artifact first.
mvn clean package -DskipTests
```
cd "$ROOT_DIR"
```

```bash
# 1) Start HBase
docker compose -p hg-hbase -f docker/hbase/docker-compose.hbase.yml down -v
docker compose -p hg-hbase -f docker/hbase/docker-compose.hbase.yml build --no-cache hbase
HBASE_MASTER_HOSTNAME=localhost HBASE_REGIONSERVER_HOSTNAME=localhost \
docker compose -p hg-hbase -f docker/hbase/docker-compose.hbase.yml up -d
until docker exec hg-hbase-test nc -z localhost 2181 >/dev/null 2>&1; do sleep 2; done
echo "HBase ZooKeeper is reachable on 2181"
# Optional troubleshooting stream:
# docker compose -p hg-hbase -f docker/hbase/docker-compose.hbase.yml logs -f hbase
```
```bash
# 2) Configure HugeGraph (standalone runtime)
SERVER_DIR="$(find . -maxdepth 4 -type d -path './hugegraph-server/apache-hugegraph-server-*' | head -n 1)"
[ -n "$SERVER_DIR" ] || { echo "Build artifact not found"; exit 1; }
CONF="$SERVER_DIR/conf/graphs/hugegraph.properties"

perl -pi -e 's/^backend=.*/backend=hbase/' "$CONF"
perl -pi -e 's/^serializer=.*/serializer=hbase/' "$CONF"
perl -pi -e 's/^#(hbase\.hosts=.*)/$1/' "$CONF"
perl -pi -e 's/^#(hbase\.port=.*)/$1/' "$CONF"
perl -pi -e 's/^#(hbase\.znode_parent=.*)/$1/' "$CONF"
perl -pi -e 's/^hbase\.hosts=.*/hbase.hosts=localhost/' "$CONF"
perl -pi -e 's/^hbase\.port=.*/hbase.port=2181/' "$CONF"
perl -pi -e 's|^hbase\.znode_parent=.*|hbase.znode_parent=/hbase|' "$CONF"

grep -E '^(backend|serializer|hbase\.)' "$CONF"
```

```bash
# 3) Init and start server
cd "$SERVER_DIR"
printf 'pa\npa\n' | ./bin/init-store.sh
./bin/start-hugegraph.sh

# 4) Verify backend logs mention hbase
cd "$ROOT_DIR"
grep -Eai 'hbase|rocksdb|hstore' "$SERVER_DIR"/logs/*.log | tail -n 30
```

</details>

<details>
<summary><b>Option 2: Docker HugeGraph (fully containerized)</b></summary>

```
cd "$ROOT_DIR"
```

```bash
# 1) Start HBase
docker compose -p hg-hbase -f docker/hbase/docker-compose.hbase.yml down -v
docker compose -p hg-hbase -f docker/hbase/docker-compose.hbase.yml build --no-cache hbase
HBASE_HOSTNAME=hbase docker compose -p hg-hbase -f docker/hbase/docker-compose.hbase.yml up -d
until docker exec hg-hbase-test nc -z localhost 2181 >/dev/null 2>&1; do sleep 2; done
echo "HBase ZooKeeper is reachable on 2181"
# Optional troubleshooting stream:
# docker compose -p hg-hbase -f docker/hbase/docker-compose.hbase.yml logs -f hbase
```

```bash
# 2) Build HugeGraph server image
docker build -f hugegraph-server/Dockerfile -t hugegraph/server:dev .

# 3) Resolve HBase network
HBASE_NETWORK="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{println $k}}{{end}}' hg-hbase-test | head -n 1)"
echo "$HBASE_NETWORK"
```

```bash
# 4) One-shot init-store
docker rm -f hg-server-init >/dev/null 2>&1 || true
docker run --rm --name hg-server-init \
  --network "$HBASE_NETWORK" \
  hugegraph/server:dev \
  bash -lc '
    set -euo pipefail
    CONF=/hugegraph-server/conf/graphs/hugegraph.properties
    perl -pi -e "s/^backend=.*/backend=hbase/" "$CONF"
    perl -pi -e "s/^serializer=.*/serializer=hbase/" "$CONF"
    perl -pi -e "s/^#(hbase\.hosts=.*)/\$1/" "$CONF"
    perl -pi -e "s/^#(hbase\.port=.*)/\$1/" "$CONF"
    perl -pi -e "s/^#(hbase\.znode_parent=.*)/\$1/" "$CONF"
    perl -pi -e "s/^hbase\.hosts=.*/hbase.hosts=hbase/" "$CONF"
    perl -pi -e "s/^hbase\.port=.*/hbase.port=2181/" "$CONF"
    perl -pi -e "s|^hbase\.znode_parent=.*|hbase.znode_parent=/hbase|" "$CONF"
    printf "pa\npa\n" | ./bin/init-store.sh
  '
```

```bash
# 5) Start HugeGraph container
docker rm -f hg-server-dev-hbase >/dev/null 2>&1 || true
docker run -d --name hg-server-dev-hbase \
  --network "$HBASE_NETWORK" \
  -p 8080:8080 \
  -p 8182:8182 \
  hugegraph/server:dev \
  bash -lc '
    set -euo pipefail
    CONF=/hugegraph-server/conf/graphs/hugegraph.properties
    perl -pi -e "s/^backend=.*/backend=hbase/" "$CONF"
    perl -pi -e "s/^serializer=.*/serializer=hbase/" "$CONF"
    perl -pi -e "s/^#(hbase\.hosts=.*)/\$1/" "$CONF"
    perl -pi -e "s/^#(hbase\.port=.*)/\$1/" "$CONF"
    perl -pi -e "s/^#(hbase\.znode_parent=.*)/\$1/" "$CONF"
    perl -pi -e "s/^hbase\.hosts=.*/hbase.hosts=hbase/" "$CONF"
    perl -pi -e "s/^hbase\.port=.*/hbase.port=2181/" "$CONF"
    perl -pi -e "s|^hbase\.znode_parent=.*|hbase.znode_parent=/hbase|" "$CONF"
    ./bin/start-hugegraph.sh -t 120
    tail -f /hugegraph-server/logs/hugegraph-server.log
  '
```

```bash
# 6) Verify hbase backend
docker exec hg-server-dev-hbase bash -lc "grep -E '^(backend|serializer|hbase\.)' /hugegraph-server/conf/graphs/hugegraph.properties"
docker exec hg-server-dev-hbase bash -lc "grep -Ei 'hbase|rocksdb|hstore' /hugegraph-server/logs/*.log | tail -n 30"
```

</details>

After either path is up, run the shared tests below.

---

## Common Testing Steps

### Apache HugeGraph Persistent Runbook (REST Engine)

### Prerequisites and Constants

- Base URL: `http://localhost:8080`
- Graph target name: `hugegraph`
- Storage backend: persistent (HBase/Cassandra/RocksDB)

---

### Step 1: Purge Database (Fresh Restart)

Wipe any conflicting test records and data schema.

```bash
curl -X DELETE "http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/clear?confirm_message=I%27m+sure+to+delete+all+data"
```

Status `204 No Content` confirms success.

---

### Step 2: Provision Structural Schema

1) Register property keys:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"name": "name", "data_type": "TEXT", "cardinality": "SINGLE"}' \
  "http://localhost:8080/graphs/hugegraph/schema/propertykeys"
```

2) Register vertex label (PRIMARY_KEY):

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"name": "person", "id_strategy": "PRIMARY_KEY", "properties": ["name"], "primary_keys": ["name"]}' \
  "http://localhost:8080/graphs/hugegraph/schema/vertexlabels"
```

3) Register edge label:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"name": "knows", "source_label": "person", "target_label": "person", "properties": []}' \
  "http://localhost:8080/graphs/hugegraph/schema/edgelabels"
```

---

### Step 3: Populate Graph Elements

1) Batch write vertices (Alice and Bob):

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '[{"label": "person", "properties": {"name": "Alice"}}, {"label": "person", "properties": {"name": "Bob"}}]' \
  "http://localhost:8080/graphs/hugegraph/graph/vertices/batch"
```

Response should include IDs similar to `1:Alice` and `1:Bob`.

2) Create directed edge (Alice knows Bob):

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"label": "knows", "outV": "1:Alice", "inV": "1:Bob", "properties": {}}' \
  "http://localhost:8080/graphs/hugegraph/graph/edges"
```

---

### Step 4: Synchronous Verification and Traversal

1) Verify target K-hop output:

```bash
curl -s "http://localhost:8080/graphs/hugegraph/traversers/kout?source=%221:Alice%22&direction=OUT&max_depth=1"
```

Expected output: `{"vertices":["1:Bob"]}`

2) Verify relation path structure:

```bash
curl -s "http://localhost:8080/graphs/hugegraph/traversers/rays?source=%221:Alice%22&direction=OUT&label=knows&max_depth=1"
```

Expected output contains: `rays":[{"objects":["1:Alice","1:Bob"]}]`

---

### Troubleshooting Cheat Sheet

- URI syntax error: do not append literal `"` inside bare URLs. Use URL-encoded values (`%22`).
- Property missing errors: prefer native `/traversers/*` APIs for synchronous reads.

---

## Cleanup

Run cleanup only after testing is complete.

### Standalone HugeGraph + Docker HBase

```bash
cd "$SERVER_DIR" && ./bin/stop-hugegraph.sh
cd "$ROOT_DIR"
docker compose -p hg-hbase -f docker/hbase/docker-compose.hbase.yml down -v
```

### Docker HugeGraph + Docker HBase

```bash
docker rm -f hg-server-init >/dev/null 2>&1 || true
docker rm -f hg-server-dev-hbase
docker compose -p hg-hbase -f docker/hbase/docker-compose.hbase.yml down -v
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `UnknownHostException: hbase:16000` | HugeGraph container is not on same Docker network as HBase. Verify `HBASE_NETWORK` and `--network`. |
| RocksDB logs in server output | `backend=rocksdb` still active; re-run backend config and restart. |
| `TableNotFoundException` on API calls | Tables not initialized; re-run `init-store.sh` from selected path. |
| Port 8182 already in use | `lsof -i :8182` then `kill <PID>`. |
| HBase container not starting | Check `lsof -i :2181`; increase Docker memory to >= 4 GB. |

---

## Verification Checklist

- [ ] `backend=hbase` in `hugegraph.properties`
- [ ] Server logs show HBase client messages (not RocksDB/HStore)
- [ ] HBase tables exist in `default_hugegraph:*`
- [ ] REST runbook queries return expected graph data
- [ ] Data survives server restart

---

## References

- HBase official docs: https://hbase.apache.org/
- HugeGraph HBase backend: `hugegraph-server/hugegraph-hbase/`
- HBase Docker Compose: `docker/hbase/docker-compose.hbase.yml`
- HBase Docker config: `docker/hbase/hbase-site.xml`
