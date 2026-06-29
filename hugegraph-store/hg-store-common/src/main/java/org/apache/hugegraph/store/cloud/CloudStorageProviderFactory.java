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

package org.apache.hugegraph.store.cloud;

import java.io.IOException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Getter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for {@link CloudStorageProvider} instances.
 *
 * <p>Providers are discovered at class-loading time via {@link ServiceLoader}.
 * Any JAR that includes
 * {@code META-INF/services/org.apache.hugegraph.store.cloud.CloudStorageProvider}
 * is automatically picked up when it is present on the classpath.
 *
 * <p>Usage:
 * <pre>
 *   CloudStorageConfig cfg = ...; // populated from application.yml
 *   CloudStorageProvider provider = CloudStorageProviderFactory.initialize(cfg);
 *   // later:
 *   CloudStorageProvider active = CloudStorageProviderFactory.getActiveProvider();
 * </pre>
 */
public final class CloudStorageProviderFactory {

    private static final Logger log = LoggerFactory.getLogger(CloudStorageProviderFactory.class);

    /** All discovered providers keyed by {@link CloudStorageProvider#providerName()}. */
    private static final Map<String, CloudStorageProvider> REGISTRY = new ConcurrentHashMap<>();

    /** The currently active (initialized) provider; null when disabled or not yet initialized.
     * -- GETTER --
     *  Returns the currently active provider, or
     *  if cloud storage is
     *  disabled or
     *  has not yet been called.
     */
    @Getter
    private static volatile CloudStorageProvider activeProvider;

    static {
        loadProviders();
    }

    private CloudStorageProviderFactory() {
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Initializes and activates the cloud storage provider described by {@code config}.
     *
     * <p>The method is idempotent: if called multiple times, the existing active
     * provider is closed before a new one is initialized.
     *
     * @param config cloud storage configuration
     * @return the initialized provider, or {@code null} when
     *         {@link CloudStorageConfig#isEnabled()} is {@code false}
     * @throws IllegalArgumentException if no provider matching {@code config.getProvider()}
     *                                  was found on the classpath
     */
    public static synchronized CloudStorageProvider initialize(CloudStorageConfig config) {
        if (!config.isEnabled()) {
            log.info("Cloud storage is disabled (cloud.storage.enabled=false)");
            return null;
        }

        String name = config.getProvider();
        CloudStorageProvider provider = REGISTRY.get(name);
        if (provider == null) {
            throw new IllegalArgumentException(
                    "No cloud storage provider found for name '" + name + "'. " +
                    "Available providers: " + REGISTRY.keySet() + ". " +
                    "Make sure the provider JAR (e.g. hg-store-cloud-s3) is on the classpath.");
        }

        // Close any previously active provider
        if (activeProvider != null && activeProvider != provider) {
            try {
                activeProvider.close();
            } catch (IOException e) {
                log.warn("Error closing previous cloud storage provider", e);
            }
        }

        provider.init(config);
        activeProvider = provider;
        log.info("Cloud storage provider '{}' initialized. bucket={}", name, config.getBucket());
        return provider;
    }

    /**
     * Shuts down and deregisters the active provider.
     * Subsequent calls to {@link #getActiveProvider()} return {@code null}.
     */
    public static synchronized void shutdown() {
        if (activeProvider != null) {
            try {
                activeProvider.close();
                log.info("Cloud storage provider '{}' closed", activeProvider.providerName());
            } catch (IOException e) {
                log.warn("Error closing cloud storage provider", e);
            }
            activeProvider = null;
        }
    }

    /**
     * Resets the active provider to {@code null} without closing it.
     *
     * <p><b>For testing only.</b> Use {@link #shutdown()} in production code.
     */
    public static synchronized void reset() {
        activeProvider = null;
    }

    /**
     * Directly injects an active provider, bypassing SPI discovery and {@link #initialize}.
     *
     * <p><b>For testing only.</b> Allows tests to supply a stub/mock provider without
     * placing a real JAR on the classpath.
     *
     * @param provider the provider instance to activate (may be {@code null} to clear)
     */
    public static synchronized void setActiveProviderForTest(CloudStorageProvider provider) {
        activeProvider = provider;
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /** Scans the classpath for {@link CloudStorageProvider} implementations via SPI. */
    private static void loadProviders() {
        ServiceLoader<CloudStorageProvider> loader =
                ServiceLoader.load(CloudStorageProvider.class,
                                   CloudStorageProviderFactory.class.getClassLoader());
        for (CloudStorageProvider p : loader) {
            String name = p.providerName();
            if (REGISTRY.containsKey(name)) {
                log.warn("Duplicate cloud storage provider name '{}' – keeping first registration",
                         name);
            } else {
                REGISTRY.put(name, p);
                log.info("Discovered cloud storage provider: '{}' ({})",
                         name, p.getClass().getName());
            }
        }
        if (REGISTRY.isEmpty()) {
            log.debug("No cloud storage providers found on classpath; cloud storage is unavailable");
        }
    }
}

