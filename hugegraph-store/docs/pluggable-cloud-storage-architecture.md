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

| Configuration                              | Default  | Description                                                                                                         |
|--------------------------------------------|----------|---------------------------------------------------------------------------------------------------------------------|
| `cloud.storage.enabled`                    | `false`  | Enables/disables cloud storage integration.                                                                         |
| `cloud.storage.provider`                   | `s3`     | Active provider name. Must match `CloudStorageProvider#providerName()`.                                            |
| `cloud.storage.bucket`                     | _(none)_ | Target object storage bucket/container. Required when enabled.                                                      |
| `cloud.storage.region`                     | _(none)_ | Provider region (for example `us-east-1`).                                                                          |
| `cloud.storage.endpoint`                   | _(none)_ | Optional custom endpoint for S3-compatible stores (MinIO/Ceph).                                                     |
| `cloud.storage.access-key`                 | _(none)_ | Access key / access ID credential.                                                                                  |
| `cloud.storage.secret-key`                 | _(none)_ | Secret key credential.                                                                                              |
| `cloud.storage.path-prefix`                | `hugegraph` | Prefix prepended to all remote object keys.                                                                      |
| `cloud.storage.startup-hydration-enabled`  | `true`   | Downloads missing remote files on DB opening before normal serving.                                                 |
| `cloud.storage.read-miss-guard-window-ms`  | `3000`   | Guard window to throttle repeated read-miss hydration attempts per db/table. Values `<= 0` disable throttling.    |
| `cloud.storage.extra-properties`           | `{}`     | Provider-specific key/value map passed through during provider initialization.                                      |

Notes:

- Keep `bucket` and `path-prefix` stable across restarts to preserve object-key continuity.
- If `provider` is not found on classpath, initialization fails fast in `CloudStorageProviderFactory`.

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

- `onTableFileCreated` upload failure throws `IllegalStateException` (fail-fast).
- This surfaces cloud-sync write risk immediately instead of silently diverging local/cloud state.

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

| Failure scenario                            | Data loss?                               | RPO                                              | Recovery mechanism                                                                                                                               | Mitigation                                                                                                      |
|---------------------------------------------|-------------------------------------------|--------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| Single Store crash                          | No                                        | 0s                                               | 2/3 Raft quorum survives. Leader re-election continues service. In-flight (not SST-flushed) data is recovered from Raft logs; flushed data remains available via local/cloud SSTs. | Keep 3+ replicas across zones, alert on replica loss, and use durable PV-backed store nodes.                  |
| 2 of 3 Stores crash                         | No (service stalls until quorum restored) | 0s                                               | Surviving replica Raft log bootstraps recovering nodes after restart. In-flight data is recovered from Raft log replay.                         | Enforce anti-affinity and failure-domain isolation to prevent correlated failures.                              |
| All Stores crash (disks intact)             | No                                        | 0s                                               | Local Raft logs replay on boot and recover non-flushed in-flight data. Local SSTs reopen and cloud sync resumes for pending uploads.          | Ensure orchestrator auto-restart, fast PV reattach, and regular restart drills.                                |
| Catastrophic loss: all Store disks destroyed | Yes                                      | Seconds to minutes (depends on SST sync mode/interval) | Raft logs and local SSTs are lost. Nodes recover from last completed cloud SST sync; data after that sync is unrecoverable.                   | Use durable disks, scheduled volume snapshots/backups, and synchronous SST upload mode for tighter RPO.        |

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
    bucket: my-container
    region: myaccount  # Azure account name (using region field)
    endpoint: https://myaccount.blob.core.windows.net
    access-key: ${NEWPROVIDER_KEY}
```

Or via Docker env:

```bash
HG_CLOUD_STORAGE_ENABLED=true \
HG_CLOUD_STORAGE_PROVIDER=newprovider \
HG_CLOUD_STORAGE_BUCKET=my-container \
HG_CLOUD_STORAGE_REGION=myaccount \
HG_CLOUD_STORAGE_ENDPOINT=https://myaccount.blob.core.windows.net \
HG_CLOUD_STORAGE_ACCESS_KEY=${NEWPROVIDER_KEY} \
docker run hugegraph-store:latest
```

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
4. **Error handling**: Throw `IOException` for operational issues; let Store handle retries/logging.
5. **Logging**: Use SLF4J (via `@Slf4j`) for consistent log levels with Store.
6. **Configuration validation**: Validate all required fields in `init()` and fail fast.

### Contributing Your Provider

To upstream your new provider (e.g., `hg-store-cloud-newprovider`):

1. Create PR to `hugegraph-store/` with new provider module
2. Add tests in `hg-store-test/` that validate provider behavior
3. Update `docker/cloud-storage/` examples with new provider setup steps
4. Update `install-dist/` license/notice if adding third-party dependencies
