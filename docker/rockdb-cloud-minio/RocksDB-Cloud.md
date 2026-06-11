# RocksDB Cloud Backend Testing with MinIO

This guide explains how to test the `rocksdb-cloud` backend locally using [MinIO](https://min.io/) as an S3-compatible object store.

> **All commands should be run from the repository root** unless otherwise noted.

---

## Architecture

```
HugeGraph Server
    └── rocksdb-cloud backend
            └── RocksDBCloudSessions (AWS SDK v2)
                    └── MinIO (S3-compatible)  <-- localhost:9000
                            └── bucket: hugegraph-rocksdb
                                    └── prefix: hugegraph/
```

---

## Quick Start (Automated)

### 1. Build the server

```bash
mvn clean package -DskipTests
```

### 2. Start MinIO

```bash
docker compose -f docker/rockdb-cloud-minio/docker-compose.minio.yml up -d
```

MinIO console: [http://localhost:9001](http://localhost:9001)  
Credentials: `minioadmin` / `minioadmin`

### 3. Run the smoke test

```bash
chmod +x docker/rockdb-cloud-minio/test-rocksdb-cloud.sh
./docker/rockdb-cloud-minio/test-rocksdb-cloud.sh
```

The script will:
- Configure `hugegraph.properties` for `rocksdb-cloud`
- Init the backend store and start HugeGraph
- Write schema + vertex data
- Read the data back via REST API and Gremlin
- Verify objects exist in the MinIO bucket

---

## Manual Setup

### Step 1: Start MinIO

```bash
docker compose -f docker/rockdb-cloud-minio/docker-compose.minio.yml up -d

# Confirm MinIO API is ready
curl -s http://localhost:9000/minio/health/live && echo "MinIO ready"

# Confirm bucket was created
docker exec hg-minio-test mc ls local/hugegraph-rocksdb
```

### Step 2: Configure HugeGraph for rocksdb-cloud

```bash
SERVER_DIR="$(find . -maxdepth 3 -type d -path './apache-hugegraph-*/apache-hugegraph-server-*' | head -n 1)"
SERVER_DIR="${SERVER_DIR#./}"
CONF="$SERVER_DIR/conf/graphs/hugegraph.properties"

# Switch to rocksdb-cloud backend
perl -pi -e 's|^backend=.*|backend=rocksdb-cloud|'   "$CONF"
perl -pi -e 's|^serializer=.*|serializer=binary|'    "$CONF"

# Set local data paths
perl -pi -e 's|^#?(rocksdb\.data_path)=.*|$1=rocksdb-cloud-data/data|' "$CONF"
perl -pi -e 's|^#?(rocksdb\.wal_path)=.*|$1=rocksdb-cloud-data/wal|'   "$CONF"

# MinIO S3 config
cat >> "$CONF" << 'EOF'
rocksdb.cloud.s3_bucket_name=hugegraph-rocksdb
rocksdb.cloud.s3_region=us-east-1
rocksdb.cloud.s3_object_prefix=hugegraph/
rocksdb.cloud.aws_access_key_id=minioadmin
rocksdb.cloud.aws_secret_access_key=minioadmin
rocksdb.cloud.s3_endpoint=http://localhost:9000
rocksdb.cloud.s3_path_style_access=true
EOF
```

### Step 3: Init store and start HugeGraph

```bash
printf 'pa\npa\n' | "$SERVER_DIR/bin/init-store.sh"
"$SERVER_DIR/bin/start-hugegraph.sh" -t 60
```

### Step 4: Write and read data

```bash
# Create schema
curl -s -u admin:pa -X POST \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/schema/propertykeys \
  -H 'Content-Type: application/json' \
  -d '{"name":"cloud_key","data_type":"TEXT","cardinality":"SINGLE","check_exist":false}' \
  | python3 -m json.tool

curl -s -u admin:pa -X POST \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/schema/vertexlabels \
  -H 'Content-Type: application/json' \
  -d '{"name":"cloud_node","id_strategy":"PRIMARY_KEY","primary_keys":["cloud_key"],"properties":["cloud_key"],"check_exist":false}' \
  | python3 -m json.tool

# Write a vertex
curl -s -u admin:pa -X POST \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices \
  -H 'Content-Type: application/json' \
  -d '{"label":"cloud_node","properties":{"cloud_key":"minio-test-v1"}}' \
  | python3 -m json.tool

# Read back
curl -s --compressed -u admin:pa \
  "http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices" \
  | python3 -m json.tool
```

### Step 5: Verify objects in MinIO

```bash
# List objects in the bucket
docker exec hg-minio-test mc ls local/hugegraph-rocksdb/hugegraph/ --recursive

# Or via the MinIO console
open http://localhost:9001
```

---

## Snapshot Upload/Download to MinIO

The `rocksdb-cloud` backend integrates with `createSnapshot`/`resumeSnapshot` via the HugeGraph API:

```bash
# Create snapshot (uploads to MinIO s3://hugegraph-rocksdb/hugegraph/snapshots/<name>/)
curl -s -u admin:pa -X POST \
  "http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/apis/gremlin" \
  -H 'Content-Type: application/json' \
  -d '{"gremlin":"hugegraph.createSnapshot(\"snap1\")","bindings":{},"language":"gremlin-groovy","aliases":{}}'

# Verify snapshot objects in MinIO
docker exec hg-minio-test mc ls local/hugegraph-rocksdb/hugegraph/snapshots/ --recursive
```

---

## MinIO Web Console

| Item | Value |
|---|---|
| URL | http://localhost:9001 |
| Username | minioadmin |
| Password | minioadmin |
| Bucket | hugegraph-rocksdb |
| Prefix | hugegraph/ |

---

## Cleanup

```bash
# Stop HugeGraph
"$SERVER_DIR/bin/stop-hugegraph.sh"

# Stop and remove MinIO container + volume
docker compose -f docker/minio/docker-compose.minio.yml down -v

# Remove local rocksdb-cloud data directory
rm -rf rocksdb-cloud-data/
```

---

## Troubleshooting

### `S3Exception: The specified bucket does not exist`
The `minio-init` container may not have finished. Check:
```bash
docker logs hg-minio-init
docker exec hg-minio-test mc ls local/
```

### `ConnectException: Connection refused` to `localhost:9000`
MinIO container is not running:
```bash
docker compose -f docker/minio/docker-compose.minio.yml ps
docker compose -f docker/minio/docker-compose.minio.yml up -d
```

### `SdkClientException: Unable to execute HTTP request`
Check `rocksdb.cloud.s3_endpoint` is set to `http://localhost:9000` (not `https`) and `rocksdb.cloud.s3_path_style_access=true`.

---

## References

- **MinIO Docs**: https://min.io/docs/minio/container/index.html
- **AWS SDK v2 S3 Client**: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/java_s3_code_examples.html
- **Docker Compose Reference**: `docker/minio/docker-compose.minio.yml`
- **RocksDB Cloud Options**: `RocksDBCloudOptions.java`

