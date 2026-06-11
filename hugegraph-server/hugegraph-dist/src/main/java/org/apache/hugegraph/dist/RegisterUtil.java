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

package org.apache.hugegraph.dist;

import java.net.URL;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.backend.serializer.SerializerFactory;
import org.apache.hugegraph.backend.store.BackendProviderFactory;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.masterelection.RoleElectionOptions;
import org.apache.hugegraph.plugin.HugeGraphPlugin;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.VersionUtil;
import org.apache.hugegraph.version.CoreVersion;
import org.slf4j.Logger;

/**
 * BREAKING CHANGE:
 * since 1.7.0, only "hstore, rocksdb, hbase, memory" are supported for backend.
 * if you want to use cassandra, mysql, postgresql, cockroachdb or palo as backend,
 * please find a version before 1.7.0 of apache hugegraph for your application.
 */
public class RegisterUtil {

    private static final Logger LOG = Log.logger(RegisterUtil.class);

    static {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("dist", DistOptions.instance());
        OptionSpace.register("masterElection", RoleElectionOptions.instance());
    }

    public static void registerBackends() {
        String confFile = "/backend.properties";
        URL input = RegisterUtil.class.getResource(confFile);
        E.checkState(input != null,
                     "Can't read file '%s' as stream", confFile);

        PropertiesConfiguration props;
        try {
            props = new Configurations().properties(input);
        } catch (ConfigurationException e) {
            throw new HugeException("Can't load config file: %s", e, confFile);
        }

        HugeConfig config = new HugeConfig(props);
        List<String> backends = config.get(DistOptions.BACKENDS);
        for (String backend : backends) {
            registerBackend(backend);
        }
    }

    private static void registerBackend(String backend) {
        switch (backend) {
            case "hbase":
                registerHBase();
                break;
            case "rocksdb":
                registerRocksDB();
                break;
            case "hstore":
                registerHstore();
                break;
            default:
                throw new HugeException("Unsupported backend type '%s'", backend);
        }
    }

    public static void registerHBase() {
        // Register config
        OptionSpace.register("hbase",
                             "org.apache.hugegraph.backend.store.hbase.HbaseOptions");
        // Register serializer
        SerializerFactory.register("hbase",
                                   "org.apache.hugegraph.backend.store.hbase.HbaseSerializer");
        // Register backend
        BackendProviderFactory.register("hbase",
                                        "org.apache.hugegraph.backend.store.hbase" +
                                        ".HbaseStoreProvider");
    }

    public static void registerRocksDB() {
        // Register config
        OptionSpace.register("rocksdb",
                             "org.apache.hugegraph.backend.store.rocksdb.RocksDBOptions");
        // Register backend
        BackendProviderFactory.register("rocksdb",
                                        "org.apache.hugegraph.backend.store.rocksdb" +
                                        ".RocksDBStoreProvider");
        BackendProviderFactory.register("rocksdbsst",
                                        "org.apache.hugegraph.backend.store.rocksdbsst" +
                                        ".RocksDBSstStoreProvider");
        // Register rocksdb-cloud (S3-backed) backend
        OptionSpace.register("rocksdb.cloud",
                             "org.apache.hugegraph.backend.store.rocksdbcloud" +
                             ".RocksDBCloudOptions");
        BackendProviderFactory.register("rocksdb-cloud",
                                        "org.apache.hugegraph.backend.store.rocksdbcloud" +
                                        ".RocksDBCloudStoreProvider");
    }

    public static void registerHstore() {
        // Register config
        OptionSpace.register("hstore",
                             "org.apache.hugegraph.backend.store.hstore.HstoreOptions");
        // Register backend
        BackendProviderFactory.register("hstore",
                                        "org.apache.hugegraph.backend.store.hstore.HstoreProvider");
    }

    public static void registerServer() {
        // Register ServerOptions (rest-server)
        OptionSpace.register("server", "org.apache.hugegraph.config.ServerOptions");
        // Register RpcOptions (rpc-server)
        OptionSpace.register("rpc", "org.apache.hugegraph.config.RpcOptions");
        // Register AuthOptions (auth-server)
        OptionSpace.register("auth", "org.apache.hugegraph.config.AuthOptions");
    }

    /**
     * Scan the jars in plugins directory and load them
     */
    public static void registerPlugins() {
        ServiceLoader<HugeGraphPlugin> plugins = ServiceLoader.load(HugeGraphPlugin.class);
        for (HugeGraphPlugin plugin : plugins) {
            LOG.info("Loading plugin {}({})",
                     plugin.name(), plugin.getClass().getCanonicalName());
            String minVersion = plugin.supportsMinVersion();
            String maxVersion = plugin.supportsMaxVersion();

            if (!VersionUtil.match(CoreVersion.VERSION, minVersion,
                                   maxVersion)) {
                assert CoreVersion.VERSION != null;
                LOG.warn("Skip loading plugin '{}' due to the version range " +
                         "'[{}, {})' that it's supported doesn't cover " +
                         "current core version '{}'", plugin.name(),
                         minVersion, maxVersion, CoreVersion.VERSION.get());
                continue;
            }
            try {
                plugin.register();
                LOG.info("Loaded plugin '{}'", plugin.name());
            } catch (Exception e) {
                throw new HugeException("Failed to load plugin '%s'", plugin.name(), e);
            }
        }
    }
}
