/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.rocksdb.access.cloud;

import org.apache.hugegraph.config.HugeConfig;

/**
 * CloudStorageProvider is a factory interface for creating CloudStorageClient instances.
 * Implementations are discovered via Java ServiceLoader mechanism. To add a new provider:
 * 1. Create an implementation class in a JAR
 * 2. Create META-INF/services/org.apache.hugegraph.rocksdb.access.cloud.CloudStorageProvider
 * 3. Add the fully qualified class name to the services file
 * 4. Add the JAR to the classpath
 * The provider will be automatically discovered and available for use.
 */
public interface CloudStorageProvider {

    /**
     * Get the name of the cloud provider this factory creates clients for.
     * E.g., "s3", "azure", "gcs"
     *
     * @return provider name (must be unique across all providers)
     */
    String name();

    /**
     * Create a CloudStorageClient instance for the given configuration.
     *
     * @param config HugeConfig containing cloud storage configuration
     * @return configured CloudStorageClient instance ready for use
     * @throws IllegalArgumentException if required configuration is missing or invalid
     * @throws Exception if client initialization fails
     */
    CloudStorageClient create(HugeConfig config) throws Exception;
}

