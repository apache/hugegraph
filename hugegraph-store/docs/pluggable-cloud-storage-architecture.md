# Pluggable Cloud Storage Architecture (HStore)

This document explains:

- The existing HStore workflow (without cloud storage)
- The new pluggable cloud storage workflow (cloud-backed SST lifecycle)
- Write path and read path behavior
- Failure handling and operational recovery scenarios

## Overview

HStore keeps partition data in local RocksDB on each Store node. The pluggable cloud-storage
feature extends this by mirroring SST file lifecycle events (create/delete) to an external
object store and hydrating missing files back when needed (startup and read-miss).

- Existing path: local RocksDB is the primary online read/write path.
- New extension: cloud object storage acts as a pluggable SST mirror and recovery source.
- Provider model: runtime-selected SPI provider via `CloudStorageProviderFactory`.

## Architecture Diagram

```text
+---------------------------+
| Client Layer              |
| Gremlin/REST/Cypher       |
+-------------|-------------+
              v
+---------------------------+   +----------------+
| HugeGraph Server          |-->| PD Cluster     |			
+-------------|-------------+   +----------------+
              v
+----------------------------------------------+
| Store Cluster (Raft replication)             |
|                                              |
| +-----------------------------------------+  |
| | WAL + MemTable -> Local RocksDB SST     |  |
| +--------------------+--------------------+  |
|                      |                       |
|                      v                       |
| +-----------------------------------------+  |
| | Pluggable Cloud Storage Workflow (NEW)  |  |
| | CloudStorageEventListener (NEW)         |  |
| | -> CloudStorageProviderFactory (NEW)    |  |
| | -> CloudStorageProvider (NEW)           |  |
| +-----------------------------------------+  |
+----------------------|-----------------------+
                       v                                                  
        +-------------------------------+                                   
        | Cloud Storage (NEW)           |                                   
        | +-------------+ +----+ +----+ |                             
        | |S3 compatible| |ADLS| |GCP | |                              
        | +-------------+ +----+ +----+ |  
        +-------------------------------+                                

```

- `Client Layer`: External clients issuing Gremlin/REST/Cypher read-write requests.
- `HugeGraph Server`: API/query layer that routes graph requests to PD and Store.
- `PD Cluster`: Placement/metadata control plane (partition mapping, leader scheduling).
- `Store Cluster`: Raft-based data plane where writes are replicated and persisted.
- `WAL + MemTable -> Local RocksDB SST`: Local durability and compaction pipeline in each Store node.
- `Pluggable Cloud Storage Workflow (NEW)`: SST lifecycle hook path for upload/delete/download/list.
- `CloudStorageEventListener (NEW)`: Captures RocksDB file events and triggers cloud operations.
- `CloudStorageProviderFactory (NEW)`: SPI loader that selects and initializes the active provider.
- `CloudStorageProvider (NEW)`: Provider implementation (`s3`, etc.) used for object operations.
- `Cloud Storage (NEW)`: Remote object backend options (S3 compatible, ADSL, GCP).


## New Configuration Options

The pluggable cloud-storage behavior is controlled from `application.yml` under
`cloud.storage`.

| Configuration                                 | Default     | Description                                                                                                          |
|-----------------------------------------------|-------------|----------------------------------------------------------------------------------------------------------------------|
| `cloud.storage.enabled`                       | `false`     | Enables/disables cloud storage integration.                                                                          |
| `cloud.storage.provider`                      | `s3`        | Active provider name. Must match `CloudStorageProvider#providerName()`.                                              |
| `cloud.storage.path-prefix`                   | `hugegraph` | Prefix prepended to all remote object keys.                                                                          |
| `cloud.storage.startup-hydration-enabled`     | `true`      | Downloads missing remote files on DB opening before normal serving.                                                  |
| `cloud.storage.read-miss-guard-window-ms`     | `3000`      | Guard window to throttle repeated read-miss hydration attempts per db/table. Values `<= 0` disable throttling.       |
| `cloud.storage.upload-retry-max-attempts`     | `0`         | Whole-file retry attempts after a first upload failure. **Default `0` = no retries; failures go directly to DLQ.** The provider handles its own internal retries (e.g. S3 multipart-part-retry). Set `> 0` only for providers without built-in retry logic. |
| `cloud.storage.upload-retry-initial-delay-ms` | `1000`      | Delay before first whole-file retry; subsequent retries use exponential backoff. Only used when `upload-retry-max-attempts > 0`. |
| `cloud.storage.upload-retry-max-delay-ms`     | `60000`     | Upper bound for exponential backoff delay between whole-file retry attempts. Only used when `upload-retry-max-attempts > 0`. |

Notes:

- Keep `path-prefix` stable across restarts to preserve object-key continuity.
- If `provider` is not found on classpath, initialization fails fast in `CloudStorageProviderFactory`.
- Upload retry uses a two-layer model: S3 part-level retries (inside one `uploadFile()` call) are handled by the provider; whole-file retries are handled by `CloudUploadRetryQueue` only when `upload-retry-max-attempts > 0`.
- Failed uploads that exhaust all attempts (or with `maxAttempts=0`) are moved to the dead-letter queue (DLQ) persisted at `<data-path>/.cloud-upload-dlq.tsv`.
- Provider-specific properties (e.g., bucket, region, credentials) are configured under `cloud.storage.<provider>.*` namespace.

### S3 provider-specific options (`cloud.storage.s3.*`)

For provider `s3`, configure these properties under `cloud.storage.s3`:

| Configuration                                   | Default  | Description                                                                                         |
|--------------------------------------------------|----------|-----------------------------------------------------------------------------------------------------|
| `cloud.storage.s3.bucket`                       | _(none)_ | Target S3 bucket name. Required when S3 provider is enabled.                                        |
| `cloud.storage.s3.region`                       | _(none)_ | AWS region (for example `us-east-1`).                                                               |
| `cloud.storage.s3.endpoint`                     | _(none)_ | Optional custom endpoint for S3-compatible stores (MinIO/Ceph).                                     |
| `cloud.storage.s3.access-key`                   | _(none)_ | Access key / AWS Access ID credential. Omit to use AWS default credentials chain.                   |
| `cloud.storage.s3.secret-key`                   | _(none)_ | Secret key credential. Omit to use AWS default credentials chain.                                   |
| `cloud.storage.s3.multipart-part-retry-max-attempts`    | `3`     | Max retries for each multipart upload part before the whole file upload fails.                      |
| `cloud.storage.s3.multipart-part-retry-base-backoff-ms` | `1000`  | Base backoff for part retries (exponential: 1x/2x/4x...).                                           |
| `cloud.storage.s3.multipart-exhausted-direct-dlq`       | `false` | If `true`, part-retry exhaustion is marked non-retryable so outer SST retry can go directly to DLQ. |

### Sample `application.yml` (`cloud.storage`)

If you want to configure Store directly through `application.yml` (instead of env injection),
use a snippet like this:

```yaml
cloud:
  storage:
    enabled: true
    provider: s3
    path-prefix: hugegraph

    # Hydration controls
    startup-hydration-enabled: true
    read-miss-guard-window-ms: 3000

    # S3 provider-specific configuration (cloud.storage.s3.*)
    s3:
      bucket: hugegraph-store0
      region: us-east-1
      endpoint: http://minio:9000
      access-key: minioadmin
      secret-key: minioadmin
      
      # Multipart upload retry configuration (S3 part-level, inside one uploadFile() call)
      multipart-part-retry-max-attempts: 3
      multipart-part-retry-base-backoff-ms: 1000
      multipart-exhausted-direct-dlq: false
```

Notes:

- `upload-retry-max-attempts` defaults to `0` (DLQ-only, no whole-file retries) and is omitted here.
  Only add it if using a provider without built-in retry logic.
- In this docker stack, each Store node uses its own bucket (`hugegraph-store0/1/2`).
- For local MinIO, endpoint is typically `http://minio:9000` inside the docker network.
- Provider-specific properties are configured under `cloud.storage.<provider>.*` namespace.


## 3) Write Path

```text
WRITE PATH (ANSI)

Client
  |
  | 1) Write request
  v
HugeGraph Server
  |
  | 2) Route to partition leader
  v
Store Node (RocksDB)
  |
  | 3) WAL append + MemTable update
  | 4) Flush/compaction creates *.sst
  v
CloudStorageEventListener
  |
  | 5) onTableFileCreated(db, cf, file)
  | 6) uploadFile(localPath, remoteKey)
  v
CloudStorageProvider
  |
  | 7) PUT object
  v
Object Storage
  |
  | 8) ACK
  v
CloudStorageProvider -> CloudStorageEventListener (success)
```

### Write-path notes

- On DB creation (`onDBCreated`), existing local SST files are scanned and uploaded if missing in cloud.
- An async flush is triggered so WAL-recovered/in-memory data materializes into SST and gets uploaded.
- Remote object key is derived from local path relative to Store data root.

## 4) Read Path

Two hydration modes exist:

1. **Startup pre-hydration (`onDBOpening`)**: download missing remote files before serving.
2. **Read-miss hydration (`onReadMiss`)**: if local read misses, fetch missing SST from cloud, ingest, and retry.

```text
READ PATH (ANSI)

Read request
  |
  | 1) get(key)
  v
RocksDB
  |
  | 2) miss -> onReadMiss(session, table, key)
  v
CloudStorageEventListener
  |
  | 3) listFiles(dbPrefix)
  v
CloudStorageProvider
  |
  | 4) LIST objects
  v
Object Storage
  |
  | 5) return key list
  v
CloudStorageProvider
  |
  | 6) for each missing *.sst:
  |    downloadFile(remoteKey, localPath)
  |    GET object -> receive object bytes
  v
CloudStorageEventListener
  |
  | 7) ingestSstFile(downloaded)
  v
RocksDB
  |
  | 8) retry get(key)
  v
Read request result
```

### Read-path notes

- A guard window (`read-miss-guard-window-ms`) throttles repeated hydration attempts for the same db/table pair.
- Only missing local SST files are downloaded.
- Non-SST objects are ignored for read-miss ingestion.

## 5) Failure Handling

### Upload failures (write-critical)

#### Two-layer retry model

Upload failures use a two-layer retry strategy:

| Layer | Where | What it retries | Config keys |
|-------|-------|-----------------|-------------|
| **Part-level** (S3 only) | Inside `S3CloudStorageProvider.uploadFile()` | Individual 512 MB multipart chunks | `cloud.storage.s3.multipart-part-retry-max-attempts`, `multipart-part-retry-base-backoff-ms` |
| **File-level** (common) | `CloudUploadRetryQueue` (async) | Whole SST file after `uploadFile()` throws | `cloud.storage.upload-retry-max-attempts`, `upload-retry-initial-delay-ms`, `upload-retry-max-delay-ms` |

The default `upload-retry-max-attempts=0` disables whole-file retries. The provider (S3) handles all real retry logic internally. The common queue exists solely as a DLQ safety net.

#### Failure flow (default `upload-retry-max-attempts=0`)

- `onTableFileCreated` calls `provider.uploadFile()`.
- S3 retries individual parts internally (via `multipart-part-retry-*`).
- If `uploadFile()` still throws, failure is caught (JNI callback cannot propagate exceptions).
- `CloudUploadRetryQueue.submit()` is called -> because `maxAttempts=0`, the task goes directly to DLQ.
- DLQ is persisted to `<data-path>/.cloud-upload-dlq.tsv` and survives restarts.

#### Accessing the DLQ

DLQ entries are persisted on disk and can be inspected directly:

```bash
cat <app.data-path>/.cloud-upload-dlq.tsv
```

Format: `failedAt \t attemptCount \t dbName \t cfName \t filePath \t remoteKey \t lastError`

To replay DLQ entries, use the in-process `CloudUploadRetryQueue.replayDlq()` path from Store runtime code.

If whole-file retries are needed (e.g. for a provider without internal retry), set:

```yaml
cloud.storage.upload-retry-max-attempts: 3   # enable 3 whole-file retry attempts
cloud.storage.upload-retry-initial-delay-ms: 1000
cloud.storage.upload-retry-max-delay-ms: 60000
```

### Delete failures (non-fatal)

- `onTableFileDeleted` delete failure is logged and processing continues.
- Impact is stale/orphaned cloud objects, not immediate read/write unavailability.

### Hydration failures

- Pre-hydration/list/download failures throw `IllegalStateException` and stop the flow for that DB open attempt.
- Read-miss hydration failures are logged and return `false`; caller falls back to original miss behavior.

### Provider lifecycle / config failures

- Unknown provider name or missing plugin JAR fails initialization in `CloudStorageProviderFactory`.
- Provider switching/re-init is handled with close-and-reinitialize semantics.

## 6) Recovery Scenarios

Failure-mode and RPO-oriented recovery summary:

| Failure scenario                              | Data loss?                                 | RPO                                                    | Recovery mechanism                                                                                                                                                                 | Mitigation                                                                                                      |
|-----------------------------------------------|--------------------------------------------|--------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| Single Store crash                            | No                                         | 0s                                                     | 2/3 Raft quorum survives. Leader re-election continues service. In-flight (not SST-flushed) data is recovered from Raft logs; flushed data remains available via local/cloud SSTs. | Keep 3+ replicas across zones, alert on replica loss, and use durable PV-backed store nodes.                    |
| 2 of 3 Stores crash                           | No (service stalls until quorum restored)  | 0s                                                     | Surviving replica Raft log bootstraps recovering nodes after restart. In-flight data is recovered from Raft log replay.                                                            | Enforce anti-affinity and failure-domain isolation to prevent correlated failures.                              |
| Catastrophic loss: all Store disks destroyed  | Yes                                        | Seconds to minutes (depends on SST sync mode/interval) | Raft logs and local SSTs are lost. Nodes recover from last completed cloud SST sync; data after that sync is unrecoverable.                                                        | Use durable disks, scheduled volume snapshots/backups, and synchronous SST upload mode for tighter RPO.         |

Notes:

- In-flight data (not yet flushed to SST) is recovered from Raft logs when quorum-replicated.
- Cloud storage recovery primarily protects flushed SST state and disaster cases involving local disk loss.
- Synchronous SST upload reduces catastrophic-loss RPO compared with periodic upload mode.

## 7) Operational Notes

- Prefer stable `path-prefix` and bucket naming; changing them affects object lookup continuity.
- Keep plugin JAR versions aligned with Store version to avoid SPI/API mismatch.
- Track logs around upload, hydration, and provider init to detect divergence early.
- For DR drills, test both node-level restart and full-cluster restart with cloud hydration enabled.

## Plugin Development: Adding Custom Cloud Storage Providers

This section guides developers on building and deploying new cloud storage provider plugins (e.g., Azure Blob Storage, ADLS, GCP).

### Plugin Architecture Overview

HugeGraph uses a **Service Provider Interface (SPI)** pattern to discover and load cloud storage providers at runtime:

1. **CloudStorageProvider interface**: Located in `hugegraph-store/hg-store-common`, defines the contract all providers must implement.
2. **ServiceLoader discovery**: Store node uses `java.util.ServiceLoader` to find all `CloudStorageProvider` implementations on the classpath.
3. **SPI selection**: At Store startup, `CloudStorageProviderFactory` loads the provider specified in `cloud.storage.provider` config.
4. **Plugin packaging/runtime classpath**: Provider implementations are packaged as separate JAR modules and must be available on Store runtime classpath.

### CloudStorageProvider Interface

All custom providers must implement:

```java
public interface CloudStorageProvider {
    
    /**
     * Unique name for this provider (e.g., "s3", "azure", "adls").
     * Must match the `cloud.storage.provider` config value to be activated.
     */
    String providerName();
    
    /**
     * Initialize the provider with configuration.
     * Called once at Store startup. Throw exception if init fails.
     */
    void init(CloudStorageConfig config) throws IOException;
    
    /**
     * Upload a local file to cloud storage with the given remote key.
     */
    void uploadFile(String localPath, String remoteKey) throws IOException;
    
    /**
     * Download a remote file from cloud storage to the local path.
     */
    void downloadFile(String remoteKey, String localPath) throws IOException;
    
    /**
     * Delete a remote file from cloud storage.
     */
    void deleteFile(String remoteKey) throws IOException;
    
    /**
     * Check if a remote file exists.
     */
    boolean fileExists(String remoteKey) throws IOException;
    
    /**
     * List all remote files under the given directory prefix.
     * Returns list of keys (with prefix stripped).
     */
    List<String> listFiles(String remoteDirPrefix) throws IOException;
    
    /**
     * Close and release all provider resources.
     */
    void close() throws IOException;
}
```

**Location:** `hugegraph-store/hg-store-common/src/main/java/org/apache/hugegraph/store/cloud/CloudStorageProvider.java`

### Module Structure for a New Provider

```
hugegraph-store/
├── hg-store-cloud-newprovider/                             # New provider module
│   ├── pom.xml                                              # Maven module definition
│   ├── src/main/java/org/apache/hugegraph/store/cloud/newprovider/
│   │   └── NewProviderCloudStorageProvider.java             # Implementation
│   ├── src/main/resources/META-INF/services/
│   │   └── org.apache.hugegraph.store.cloud.CloudStorageProvider
│   └── src/test/java/...                                    # Unit tests
```

Recommendation:

- For providers maintained in this repository, prefer adding them as submodules under `hugegraph-store/` (same model as `hg-store-cloud-s3`).
- External providers are also supported if their jars are placed on runtime classpath.

### Step 1: Create Module POM

File: `hugegraph-store/hg-store-cloud-newprovider/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    
    <parent>
        <groupId>org.apache.hugegraph</groupId>
        <artifactId>hugegraph-store</artifactId>
        <version>${revision}</version>
        <relativePath>../pom.xml</relativePath>
    </parent>
    
    <artifactId>hg-store-cloud-newprovider</artifactId>
    <name>HugeGraph Store Cloud NewProvider</name>
    <description> NewProvider Storage plugin for HugeGraph Store cloud storage integration</description>
    
    <dependencies>
        <!-- HugeGraph Store cloud SPI -->
        <dependency>
            <groupId>org.apache.hugegraph</groupId>
            <artifactId>hg-store-common</artifactId>
            <version>${revision}</version>
        </dependency>
        
        <!-- NewProvider SDK -->
        <dependency>
            <groupId>NewProvider</groupId>
            <artifactId>NewProvider</artifactId>
            <version>1.0</version>
        </dependency>
        <!-- Optional: additional provider-specific dependencies -->
    </dependencies>
</project>
```

### Step 2: Implement the Provider

File: `hugegraph-store/hg-store-cloud-newprovider/src/main/java/org/apache/hugegraph/store/cloud/newprovider/NewProviderCloudStorageProvider.java`

```java
package org.apache.hugegraph.store.cloud.newprovider;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("RedundantThrows")
@Slf4j
public class NewProviderCloudStorageProvider implements CloudStorageProvider {
    
    public static final String PROVIDER_NAME = "newprovider";
    
    private Object providerClient;
    private String pathPrefix;
    
    @Override
    public String providerName() {
        return PROVIDER_NAME;
    }
    
    @Override
    public void init(CloudStorageConfig config) throws IOException {
        //Steps to initialize the NewProvider client using config parameters
    }
    
    @Override
    public void uploadFile(String localPath, String remoteKey) throws IOException {
        //Steps to upload file to NewProvider cloud storage
    }
    
    @Override
    public void downloadFile(String remoteKey, String localPath) throws IOException {
        //Steps to download file from NewProvider cloud storage
    }
    
    @Override
    public void deleteFile(String remoteKey) throws IOException {
        //Steps to delete file from NewProvider cloud storage
    }
    
    @Override
    public boolean fileExists(String remoteKey) throws IOException {
        //Steps to check if file exists in NewProvider cloud storage
        return false;
    }
    
    @Override
    public List<String> listFiles(String remoteDirPrefix) throws IOException {
        //Steps to list files in NewProvider cloud storage
        return new ArrayList<>();
    }
    
    @Override
    public void close() throws IOException {
        //Steps to close and cleanup NewProvider client resources
    }
}
```

### Step 3: Register via SPI

File: `hugegraph-store/hg-store-cloud-newprovider/src/main/resources/META-INF/services/org.apache.hugegraph.store.cloud.CloudStorageProvider`

```
org.apache.hugegraph.store.cloud.newprovider.NewProviderCloudStorageProvider
```

This file tells Java's `ServiceLoader` to discover this provider at runtime.

### Step 4: Build and Test

```bash
# Build the provider module
cd hugegraph-store
mvn clean package -pl hg-store-cloud-newprovider -am -DskipTests

# Find the generated JAR
find . -name "hg-store-cloud-newprovider-*.jar"
# Output: hugegraph-store/hg-store-cloud-newprovider/target/hg-store-cloud-newprovider-1.0.jar
```

### Step 5: Package for Runtime Classpath

Use one of these runtime models:

**Option A (recommended for in-repo providers): add as Store submodule dependency**

1. Add module `hg-store-cloud-newprovider` under `hugegraph-store/`.
2. Add dependency from `hg-store-node` to `hg-store-cloud-newprovider`.
3. Rebuild distribution so provider + transitive dependencies are packaged with Store runtime.

**Option B (external provider): provide jars on runtime classpath**

- Supply `hg-store-cloud-newprovider-*.jar` **and** all required dependency jars, or
- supply a single shaded/uber provider jar that already contains transitive dependencies.

Notes:

- Simply copying a provider jar without its runtime dependencies can cause `ClassNotFoundException` during SPI loading.
- The exact classpath injection method depends on your launch model (custom startup script, container image, or JVM args).

### Step 6: Configure and Activate

In Store `application.yml`:

```yaml
cloud:
  storage:
    enabled: true
    provider: newprovider  # Must match NewProviderCloudStorageProvider.providerName()
    path-prefix: hugegraph
    
    # Provider-specific configuration (cloud.storage.newprovider.*)
    newprovider:
      bucket: my-container
      region: myaccount  # e.g., Azure account name
      endpoint: https://myaccount.blob.core.windows.net
      access-key: ${NEWPROVIDER_KEY}
      secret-key: ${NEWPROVIDER_SECRET}
```

Or via Docker env (for S3 example):

```bash
HG_CLOUD_STORAGE_ENABLED=true \
HG_CLOUD_STORAGE_PROVIDER=s3 \
HG_CLOUD_STORAGE_PATH_PREFIX=hugegraph \
HG_CLOUD_STORAGE_S3_BUCKET=hugegraph-store0 \
HG_CLOUD_STORAGE_S3_REGION=us-east-1 \
HG_CLOUD_STORAGE_S3_ENDPOINT=http://minio:9000 \
HG_CLOUD_STORAGE_S3_ACCESS_KEY=minioadmin \
HG_CLOUD_STORAGE_S3_SECRET_KEY=minioadmin \
docker run hugegraph-store:latest
```

Or via Docker env (for custom newprovider):

```bash
HG_CLOUD_STORAGE_ENABLED=true \
HG_CLOUD_STORAGE_PROVIDER=newprovider \
HG_CLOUD_STORAGE_PATH_PREFIX=hugegraph \
HG_CLOUD_STORAGE_NEWPROVIDER_BUCKET=my-container \
HG_CLOUD_STORAGE_NEWPROVIDER_REGION=myaccount \
HG_CLOUD_STORAGE_NEWPROVIDER_ENDPOINT=https://myaccount.blob.core.windows.net \
HG_CLOUD_STORAGE_NEWPROVIDER_ACCESS_KEY=${NEWPROVIDER_KEY} \
HG_CLOUD_STORAGE_NEWPROVIDER_SECRET_KEY=${NEWPROVIDER_SECRET} \
docker run hugegraph-store:latest
```

Notes on property naming conventions:

- **YAML properties**: Use kebab-case with provider-specific namespacing (e.g., `cloud.storage.s3.bucket`, `cloud.storage.newprovider.access-key`)
- **Environment variables**: Use uppercase with underscores and `HG_CLOUD_STORAGE_*` prefix. Provider-specific options use `HG_CLOUD_STORAGE_<PROVIDER>_*` pattern
- Kebab-case YAML properties are converted to uppercase with underscores (e.g., `multipart-part-retry-max-attempts` → `MULTIPART_PART_RETRY_MAX_ATTEMPTS`)

### Testing Your Provider

Add unit tests in `hg-store-cloud-newprovider/src/test/`:

```java
@Test
public void testInit() {
    // Test provider initialization with mock config
}

@Test
public void testUploadDownload() {
    // Test upload and download of a sample file
}

@Test
public void testDeleteAndList() {
    // Test delete and list operations
}

@Test
public void testFileExists() {
    // Test file existence check
}

@Test
public void testClose() {
    // Test provider close and resource cleanup
}
```

### Troubleshooting Provider Discovery

If your provider isn't loaded:

1. **Check SPI registration file exists:**
   ```bash
   jar tf hg-store-cloud-newprovider-1.0.jar | grep "META-INF/services"
   ```
   Should show: `META-INF/services/org.apache.hugegraph.store.cloud.CloudStorageProvider`

2. **Check provider name matches config:**
   ```bash
   jar xf hg-store-cloud-newprovider-1.0.jar META-INF/services/org.apache.hugegraph.store.cloud.CloudStorageProvider
   cat META-INF/services/org.apache.hugegraph.store.cloud.CloudStorageProvider
   ```

3. **Enable debug logging:**
   ```yaml
   logging:
     level:
       org.apache.hugegraph.store.cloud: DEBUG
       org.apache.hugegraph.store.node.cloud: DEBUG
   ```

4. **Verify provider jars are on classpath:**
   ```bash
   # Check Store startup logs for provider discovery / class-loading errors
   docker logs hugegraph-store | grep -i "ServiceLoader\|provider"
   ```

### Key Implementation Tips

1. **Thread safety**: Ensure provider is thread-safe for concurrent upload/download/delete operations.
2. **Connection pooling**: Reuse client connections; initialize once in `init()`, close in `close()`.
3. **Path normalization**: Always use `pathPrefix` correctly (see S3 example for reference).
4. **Error handling**: Throw `IOException` for operational issues. Implement your own internal retry logic (see `S3CloudStorageProvider` for reference). The common `CloudUploadRetryQueue` provides a DLQ safety net but does not retry by default (`upload-retry-max-attempts=0`).
5. **Logging**: Use SLF4J (via `@Slf4j`) for consistent log levels with Store.
6. **Configuration validation**: Validate all required fields in `init()` and fail fast.

### Contributing Your Provider

To upstream your new provider (e.g., `hg-store-cloud-newprovider`):

1. Create PR to `hugegraph-store/` with new provider module
2. Add tests in `hg-store-test/` that validate provider behavior
3. Update `docker/cloud-storage/` examples with new provider setup steps
4. Update `install-dist/` license/notice if adding third-party dependencies
