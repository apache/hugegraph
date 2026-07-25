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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

import org.apache.hugegraph.rocksdb.access.DBStoreException;

import lombok.extern.slf4j.Slf4j;

/**
 * CloudStorageRegistry manages all available cloud storage providers.
 * This registry uses Java's ServiceLoader to automatically discover and load
 * CloudStorageProvider implementations from the classpath. This enables a
 * true plugin architecture where new providers can be added by simply adding
 * their JAR to the classpath.
 * Usage:
 * <pre>
 *     // Get a client for a specific provider
 *     CloudStorageClient client = CloudStorageRegistry.getInstance()
 *         .getClient("s3", config);
 *
 *     // List all available providers
 *     List<String> providers = CloudStorageRegistry.getInstance()
 *         .listProviders();
 * </pre>
 */
@Slf4j
public final class CloudStorageRegistry {

    private static final CloudStorageRegistry INSTANCE = new CloudStorageRegistry();

    private final Map<String, CloudStorageProvider> providers = new HashMap<>();
    private boolean initialized = false;

    private CloudStorageRegistry() {
    }

    /**
     * Get the singleton registry instance.
     *
     * @return CloudStorageRegistry instance
     */
    public static CloudStorageRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Get a CloudStorageClient for the specified provider.
     * Lazily loads providers via ServiceLoader on first access.
     *
     * @param providerName the name of the provider (e.g., "s3", "azure", "gcs")
     * @param config HugeConfig with provider-specific configuration
     * @return initialized CloudStorageClient for the provider
     */
    public synchronized CloudStorageClient getClient(String providerName,
                                                      org.apache.hugegraph.config.HugeConfig config) {
        Objects.requireNonNull(providerName, "providerName cannot be null");
        Objects.requireNonNull(config, "config cannot be null");

        // Lazy load providers on first access
        if (!initialized) {
            loadProviders();
        }

        CloudStorageProvider provider = providers.get(providerName);
        if (provider == null) {
            String available = String.join(", ", providers.keySet());
            throw new DBStoreException(
                    "Cloud storage provider '%s' not found. Available providers: %s",
                    providerName, available);
        }

        try {
            return provider.create(config);
        } catch (Exception e) {
            throw new DBStoreException(
                    "Failed to create client for provider '%s': %s",
                    providerName, e.getMessage());
        }
    }

    /**
     * Get a list of all available provider names.
     *
     * @return list of provider names (lazy loads providers on first call)
     */
    public synchronized List<String> listProviders() {
        if (!initialized) {
            loadProviders();
        }
        return new ArrayList<>(providers.keySet());
    }

    /**
     * Check if a provider is available.
     *
     * @param providerName the name of the provider
     * @return true if the provider is available
     */
    public synchronized boolean isProviderAvailable(String providerName) {
        if (!initialized) {
            loadProviders();
        }
        return providers.containsKey(providerName);
    }

    /**
     * Load all available providers via ServiceLoader.
     * This is called automatically on first access.
     */
    private void loadProviders() {
        if (initialized) {
            return;
        }

        log.info("Discovering CloudStorageProvider implementations via ServiceLoader");

        try {
            ServiceLoader<CloudStorageProvider> loader =
                    ServiceLoader.load(CloudStorageProvider.class);

            for (CloudStorageProvider provider : loader) {
                String name = provider.name();
                if (name == null || name.trim().isEmpty()) {
                    log.warn("CloudStorageProvider returned null or empty name, skipping: {}",
                             provider.getClass().getName());
                    continue;
                }

                if (providers.containsKey(name)) {
                    log.warn("Duplicate CloudStorageProvider for '{}': {} (ignoring, using first)",
                             name, provider.getClass().getName());
                    continue;
                }

                providers.put(name, provider);
                log.info("Registered CloudStorageProvider: {} ({})",
                         name, provider.getClass().getName());
            }
        } catch (Exception e) {
            log.warn("Error loading CloudStorageProvider implementations via ServiceLoader: {}",
                     e.getMessage());
        }

        initialized = true;

        if (providers.isEmpty()) {
            log.warn("No CloudStorageProvider implementations found. " +
                    "This is expected if you haven't added any cloud storage JARs to the classpath.");
        } else {
            log.info("CloudStorageRegistry initialized with {} provider(s): {}",
                     providers.size(), String.join(", ", providers.keySet()));
        }
    }

    /**
     * Force reload of providers (for testing purposes).
     * Usually not needed as providers are lazily loaded.
     */
    synchronized void reload() {
        this.initialized = false;
        this.providers.clear();
        loadProviders();
    }

    /**
     * Get unmodifiable map of all available providers.
     * For testing/debugging purposes.
     */
    public synchronized Map<String, CloudStorageProvider> getProviders() {
        if (!initialized) {
            loadProviders();
        }
        return Collections.unmodifiableMap(new HashMap<>(providers));
    }
}

