# HugeGraph Cloud Storage Plugin Architecture

## Overview

HugeGraph RocksDB implements a pluggable cloud storage architecture that allows support for multiple cloud providers through JAR-based plugins. This document explains how to implement a new cloud storage provider.

## Reference Example (In Repository)

Use this template as a concrete reference for folder layout, naming, and ServiceLoader registration:

This directory (`examples/cloud-storage-plugin/`) is the reference implementation.

It includes:

- `SampleCloudStorageProvider` and `SampleCloudStorageClient`
- `META-INF/services/org.apache.hugegraph.rocksdb.access.cloud.CloudStorageProvider`
- a small `ServiceLoaderSmokeMain` runner for quick discovery checks

## Steps Overview (Using Sample Plugin)

The sample plugin demonstrates all required steps:

1. **Step 1: Module Structure** → See `pom.xml` and `src/` layout here
2. **Step 2: Provider Interface** → `SampleCloudStorageProvider.java`
3. **Step 3: Client Interface** → `SampleCloudStorageClient.java`
4. **Step 4: ServiceLoader Registration** → `src/main/resources/META-INF/services/...`
5. **Step 5: Dependencies** → See `pom.xml`
6. **Step 6: Configuration** → Users set `rocksdb.cloud.provider=sample`

## Quick Start: Adding a New Cloud Storage Provider

### Step 1: Create a New Module

Create a new Maven module for your provider. Example structure:
```
hugegraph-store-cloud-azure/
├── pom.xml
├── src/
│   └── main/
│       ├── java/org/apache/hugegraph/rocksdb/access/cloud/
│       │   ├── AzureStorageProvider.java
│       │   └── AzureStorageClient.java
│       └── resources/
│           └── META-INF/services/
│               └── org.apache.hugegraph.rocksdb.access.cloud.CloudStorageProvider
```

### Step 2: Implement CloudStorageProvider Interface

**File: AzureStorageProvider.java**

```java
package org.apache.hugegraph.rocksdb.access.cloud;

import org.apache.hugegraph.config.HugeConfig;

public class AzureStorageProvider implements CloudStorageProvider {

    @Override
    public String name() {
        return "azure";
    }

     @Override
     public CloudStorageClient create(HugeConfig config) throws Exception {
         // Parse Azure-specific configuration
         String account = getString(config, "rocksdb.cloud.azure_account", "");
         String key = getString(config, "rocksdb.cloud.azure_key", "");
         String container = getString(config, "rocksdb.cloud.azure_container", "");

         // Initialize Azure client
         BlobServiceClient blobClient = new BlobServiceClientBuilder()
             .connectionString("DefaultEndpointsProtocol=https;AccountName=" + account)
             .buildClient();

        // Return client implementation
        return new AzureStorageClient(blobClient);
    }

    private static String getString(HugeConfig config, String key, String defaultValue) {
        if (config.containsKey(key)) {
            return String.valueOf(config.getProperty(key));
        }
        return defaultValue;
    }
}
```

### Step 3: Implement CloudStorageClient Interface

**File: AzureStorageClient.java**

```java
package org.apache.hugegraph.rocksdb.access.cloud;

import com.azure.storage.blob.BlobServiceClient;

public class AzureStorageClient implements CloudStorageClient {

    private final BlobServiceClient blobClient;

    public AzureStorageClient(BlobServiceClient blobClient) {
        this.blobClient = blobClient;
    }

    @Override
    public String provider() {
        return "azure";
    }

    @Override
    public void uploadDirectory(String container, String path, String localDirectory)
            throws Exception {
        // Implement Azure blob upload
        BlobContainerClient containerClient = blobClient.getBlobContainerClient(container);
        // ... implementation details
    }

    @Override
    public void uploadIncremental(String container, String path, String localDirectory) {
        // Implement incremental upload (only changed files)
        // ... implementation details
    }

    @Override
    public void downloadDirectory(String container, String path, String localDirectory) {
        // Implement Azure blob download
        // ... implementation details
    }

    @Override
    public void close() throws Exception {
        // Close Azure client connection
        blobClient.close();
    }
}
```

### Step 4: Register Provider via ServiceLoader (Inside Plugin JAR)

Create this file inside your plugin module (not in HugeGraph core source):

**File: `src/main/resources/META-INF/services/org.apache.hugegraph.rocksdb.access.cloud.CloudStorageProvider`**

Add the fully qualified class name:
```
org.apache.hugegraph.rocksdb.access.cloud.AzureStorageProvider
```

This is enough for Java `ServiceLoader` discovery. No code change is required in HugeGraph after the cloud abstraction is available.

### Step 4.1: Build and Deploy Plugin JAR

Build your plugin externally and copy the generated JAR into HugeGraph runtime classpath (typically `lib/`), then restart HugeGraph.

```bash
mvn -f hugegraph-store-cloud-azure/pom.xml clean package -DskipTests
cp hugegraph-store-cloud-azure/target/hugegraph-store-cloud-azure-*.jar /path/to/hugegraph/lib/
```

If your plugin is for Docker deployment, make sure the plugin JAR is mounted or baked into the image under the HugeGraph classpath.

### Step 5: Configure POM Dependencies

**pom.xml**

```xml
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>org.apache.hugegraph</groupId>
    <artifactId>hugegraph-store-cloud-azure</artifactId>
    <version>1.8.0</version>

    <dependencies>
        <!-- HugeGraph Core Dependencies -->
        <dependency>
            <groupId>org.apache.hugegraph</groupId>
            <artifactId>hugegraph-store-rocksdb</artifactId>
            <version>${project.version}</version>
        </dependency>

        <!-- Azure SDK -->
        <dependency>
            <groupId>com.azure</groupId>
            <artifactId>azure-storage-blob</artifactId>
            <version>12.x.x</version>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <scope>provided</scope>
        </dependency>
    </dependencies>
</project>
```

### Step 6: Configuration in hugegraph.properties

Users can now configure your provider:

```properties
# Enable cloud storage with Azure provider
rocksdb.cloud.enabled=true
rocksdb.cloud.provider=azure
rocksdb.cloud_bucket=my-container

# Azure-specific configuration
rocksdb.cloud.azure_account=myaccount
rocksdb.cloud.azure_key=mykey
rocksdb.cloud.azure_container=my-container

# Generic sync settings (same for all providers)
rocksdb.cloud.sync_interval_seconds=60
rocksdb.cloud.sync_incremental=true
rocksdb.cloud.sync_retry_max=100
```

## CloudStorageClient Interface Reference

### Methods to Implement

#### `String provider()`
Returns the provider identifier. Must be unique across all registered providers.

**Example:**
```java
@Override
public String provider() {
    return "azure";  // or "gcs", "aliyun", etc.
}
```

#### `void uploadDirectory(String container, String path, String localDirectory)`
Uploads entire directory from local filesystem to cloud storage. Replaces all existing content.

**Parameters:**
- `container`: Bucket/container name (from `rocksdb.cloud_bucket` config)
- `path`: Object prefix/path (from `rocksdb.cloud_object_prefix` config)
- `localDirectory`: Local filesystem path to upload from

**Example:**
```java
@Override
public void uploadDirectory(String container, String path, String localDirectory) {
    // List all files in localDirectory
    // Upload each file to: container/path/filename
    // Replace any existing files with same names
}
```

#### `void uploadIncremental(String container, String path, String localDirectory)`
Uploads only changed or new files. Must be more efficient than `uploadDirectory()`.

**Example:**
```java
@Override
public void uploadIncremental(String container, String path, String localDirectory) {
    // Compare local files with remote files
    // Upload only files that are new or have changed timestamps
    // Delete remote files that no longer exist locally
}
```

#### `void downloadDirectory(String container, String path, String localDirectory)`
Downloads all files from cloud storage to local filesystem.

**Example:**
```java
@Override
public void downloadDirectory(String container, String path, String localDirectory) {
    // List all objects in container/path
    // Download each object to localDirectory
    // Preserve directory structure
}
```

#### `void close() throws Exception`
Closes the client and releases resources.

**Example:**
```java
@Override
public void close() throws Exception {
    if (azureClient != null) {
        azureClient.close();
    }
}
```

## CloudStorageProvider Interface Reference

### Methods to Implement

#### `String name()`
Returns the provider name. This is what users specify in `rocksdb.cloud.provider` config.

**Must be:**
- Lowercase alphanumeric
- Unique across all registered providers
- Examples: "s3", "azure", "gcs", "aliyun", "minio"

#### `CloudStorageClient create(HugeConfig config) throws Exception`
Factory method that creates and initializes a CloudStorageClient.

**Responsibilities:**
1. Parse provider-specific configuration keys from HugeConfig
2. Validate required configuration
3. Initialize cloud provider SDK client
4. Return fully configured CloudStorageClient instance

**Example:**
```java
@Override
public CloudStorageClient create(HugeConfig config) throws Exception {
    String account = getString(config, "rocksdb.cloud.azure_account");
    if (account == null || account.isEmpty()) {
        throw new IllegalArgumentException(
            "Missing required config: rocksdb.cloud.azure_account");
    }

     BlobServiceClient client = new BlobServiceClientBuilder()
         .connectionString(connectionString)
         .buildClient();

    return new AzureStorageClient(client);
}
```

## Configuration Best Practices

### Use Consistent Key Naming
- Use `rocksdb.cloud.{provider}_*` pattern for provider-specific config
- Example: `rocksdb.cloud.azure_account`, `rocksdb.cloud.gcs_project`

### Document Required vs Optional Config
In your provider documentation, clearly state:
- Required configuration keys
- Optional configuration with defaults
- Environment variable overrides (if supported)

### Support Legacy Keys
If possible, support both new-style (`rocksdb.cloud.provider_key`) and underscore-based (`rocksdb.cloud_provider_key`) keys for backward compatibility:

```java
private static String getString(HugeConfig config, String newKey, String legacyKey, 
                                String defaultValue) {
    if (config.containsKey(newKey)) {
        return String.valueOf(config.getProperty(newKey));
    }
    if (config.containsKey(legacyKey)) {
        return String.valueOf(config.getProperty(legacyKey));
    }
    return defaultValue;
}
```

## Deployment: Adding Your Plugin JAR

### Option 1: Add to Classpath
Place your provider JAR in the HugeGraph classpath:

```bash
# Copy JAR to HugeGraph lib directory
cp hugegraph-store-cloud-azure-1.8.0.jar /path/to/hugegraph/lib/

# Start HugeGraph (providers are auto-discovered via ServiceLoader)
./bin/start-hugegraph.sh
```

### Option 2: Shade into Distribution
Include your provider in the main distribution:

```xml
<dependency>
    <groupId>org.apache.hugegraph</groupId>
    <artifactId>hugegraph-store-cloud-azure</artifactId>
    <version>${project.version}</version>
</dependency>
```

### Verification
After adding the JAR, check logs to confirm provider was loaded:

```
INFO CloudStorageRegistry - Discovering CloudStorageProvider implementations via ServiceLoader
INFO CloudStorageRegistry - Registered CloudStorageProvider: azure (org.apache.hugegraph.rocksdb.access.cloud.AzureStorageProvider)
```

Or check available providers programmatically by calling
`CloudStorageRegistry.getInstance().listProviders()` and printing the returned list
(for example: `[s3, azure, gcs]`).

## Testing Your Provider

### Unit Tests
Test configuration parsing and client creation:

```java
@Test
public void testAzureProviderCreation() throws Exception {
    HugeConfig config = new HugeConfig();
    config.set("rocksdb.cloud.azure_account", "testaccount");
    config.set("rocksdb.cloud.azure_key", "testkey");

    AzureStorageProvider provider = new AzureStorageProvider();
    CloudStorageClient client = provider.create(config);

    assertNotNull(client);
    assertEquals("azure", client.provider());
}
```

### Integration Tests
Test against containerized emulator:

```text
@Test
@DockerCompose(file = "docker-compose-azurite.yml")
public void testUploadToAzurite() {
    // Use Azurite (Azure Blob Storage emulator)
    // Test upload/download/incremental operations
}
```

### Using Emulators
- **Azure**: Azurite (https://github.com/Azure/Azurite)
- **GCS**: GCS Emulator (https://github.com/oittaa/gcp-storage-emulator)
- **S3**: MinIO (https://min.io/)

## Error Handling

Implement robust error handling in your provider:

```text
@Override
public void uploadDirectory(String container, String path, String localDirectory) 
        throws Exception {
    try {
        doUpload(container, path, localDirectory);
    } catch (AuthenticationException e) {
        throw new Exception("Azure authentication failed. Check credentials.", e);
    } catch (NotFoundException e) {
        throw new Exception("Container not found: " + container, e);
    } catch (Exception e) {
        throw new Exception("Upload failed: " + e.getMessage(), e);
    }
}

private void doUpload(String container, String path, String localDirectory)
        throws AuthenticationException, NotFoundException {
    // Upload implementation
}
```

## Example: Complete Azure Provider Implementation

See the sample provider reference implementation:
- [SampleCloudStorageProvider](SampleCloudStorage/src/main/java/org/example/hugegraph/cloud/sample/SampleCloudStorageProvider.java)
- [SampleCloudStorageClient](SampleCloudStorage/src/main/java/org/example/hugegraph/cloud/sample/SampleCloudStorageClient.java)

## Example: Complete GCS Provider Implementation  

Use the same sample plugin pattern above and replace SDK/client logic with your GCS implementation.

## Contributing Your Provider

To contribute your provider to Apache HugeGraph:

1. Follow the Apache License Header in all files
2. Add comprehensive documentation
3. Include unit and integration tests
4. Follow HugeGraph coding standards
5. Submit a pull request with your implementation

## FAQ

**Q: Can I override the default S3 provider?**
A: No, provider names must be unique. If you want an S3 variant, use a different name like "s3-compatible-v2" or "s3-enhanced".

**Q: How do I debug provider discovery?**
A: Enable DEBUG logging for CloudStorageRegistry:
```
log4j.logger.org.apache.hugegraph.rocksdb.access.cloud.CloudStorageRegistry=DEBUG
```

**Q: What happens if no provider is configured?**
A: Cloud sync is disabled by default unless `rocksdb.cloud.enabled=true`. If enabled but provider not found, initialization fails with a clear error message.

**Q: Can providers share common code?**
A: Yes. Create a base class or utility module that multiple providers can depend on. Example: `hugegraph-store-cloud-common` for shared utilities.

**Q: Do I need to support all CloudStorageClient methods?**
A: Yes, all methods are required. `uploadIncremental()` can delegate to `uploadDirectory()` if efficient delta detection is not feasible, but implement all methods.


