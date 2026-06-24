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

package org.apache.hugegraph.backend.store;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.store.memory.InMemoryDBStoreProvider;
import org.apache.hugegraph.backend.store.raft.RaftBackendStoreProvider;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * BREAKING CHANGE:
 * since 1.7.0, only "hstore, rocksdb, hbase, memory" are supported for backend.
 * if you want to use cassandra, mysql, postgresql, cockroachdb or palo as backend,
 * please find a version before 1.7.0 of apache hugegraph for your application.
 */
public class BackendProviderFactory {

    private static final Logger LOG = Log.logger(BackendProviderFactory.class);

    private static final Map<String, Class<? extends BackendStoreProvider>> providers;

    private static final List<String> ALLOWED_BACKENDS = List.of("memory", "rocksdb", "hbase",
                                                                 "hstore");

    static {
        providers = new ConcurrentHashMap<>();
    }

    public static BackendStoreProvider open(HugeGraphParams params) {
        HugeConfig config = params.configuration();
        String backend = config.get(CoreOptions.BACKEND).toLowerCase();
        BackendException.check(!StringUtils.isEmpty(params.graph().graphSpace()),
                               "GraphSpace can not be empty for '%s'",
                               config.get(CoreOptions.STORE));
        String graph = params.graph().graphSpace()
                       + "/" + config.get(CoreOptions.STORE);
        boolean raftMode = config.get(CoreOptions.RAFT_MODE);

        BackendStoreProvider provider = newProvider(config);
        if (raftMode) {
            LOG.info("Opening backend store '{}' in raft mode for graph '{}'", backend, graph);
            provider = new RaftBackendStoreProvider(params, provider);
        }
        provider.open(graph);
        return provider;
    }

    private static BackendStoreProvider newProvider(HugeConfig config) {
        String backend = config.get(CoreOptions.BACKEND).toLowerCase();
        E.checkState(ALLOWED_BACKENDS.contains(backend.toLowerCase()),
                     "backend is illegal: %s", backend);

        String graph = config.get(CoreOptions.STORE);
        if (InMemoryDBStoreProvider.matchType(backend)) {
            return InMemoryDBStoreProvider.instance(graph);
        }

        Class<? extends BackendStoreProvider> clazz = providers.get(backend);
        BackendException.check(clazz != null,
                               "Not exists BackendStoreProvider: %s", backend);

        assert BackendStoreProvider.class.isAssignableFrom(clazz);
        BackendStoreProvider instance;
        try {
            instance = clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new BackendException(e);
        }

        BackendException.check(backend.equals(instance.type()),
                               "BackendStoreProvider with type '%s' " +
                               "can't be opened by key '%s'", instance.type(), backend);
        return instance;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void register(String name, String classPath) {
        ClassLoader classLoader = BackendProviderFactory.class.getClassLoader();
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(classPath);
        } catch (Exception e) {
            throw new BackendException(e);
        }

        // Check subclass
        boolean subclass = BackendStoreProvider.class.isAssignableFrom(clazz);
        BackendException.check(subclass, "Class '%s' is not a subclass of " +
                                         "class BackendStoreProvider", classPath);

        // Check exists
        BackendException.check(!providers.containsKey(name),
                               "Exists BackendStoreProvider: %s (%s)",
                               name, providers.get(name));

        // Register class
        providers.put(name, (Class) clazz);
    }
}
