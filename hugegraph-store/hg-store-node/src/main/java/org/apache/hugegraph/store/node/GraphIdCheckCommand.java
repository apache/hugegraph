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

package org.apache.hugegraph.store.node;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hugegraph.store.upgrade.LegacyGraphIdChecker;
import org.apache.hugegraph.util.RocksDBRuntime;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.FileSystemResource;

/** Read-only command in the Store executable JAR. Does not start Spring, PD or a Raft node. */
public final class GraphIdCheckCommand {

    private static final Set<String> VALUES = Set.of("--config", "--data-path");

    private GraphIdCheckCommand() {
    }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String... args) {
        try {
            if (args.length == 1 && "--help".equals(args[0])) {
                help();
                return 0;
            }
            if (args.length == 0) {
                throw new IllegalArgumentException(
                        "Use --check to inspect a stopped Store, or --help");
            }
            Map<String, String> arguments = new HashMap<>();
            for (int i = 0; i < args.length; i++) {
                String name = args[i];
                String value = "true";
                if (VALUES.contains(name)) {
                    if (++i == args.length || args[i].startsWith("--") || args[i].isBlank()) {
                        throw new IllegalArgumentException("Missing value for " + name);
                    }
                    value = args[i];
                } else if (!"--check".equals(name)) {
                    throw new IllegalArgumentException("Unknown option " + name);
                }
                if (arguments.putIfAbsent(name, value) != null) {
                    throw new IllegalArgumentException("Repeated option " + name);
                }
            }
            if (!arguments.containsKey("--check")) {
                throw new IllegalArgumentException("Use --check to inspect a stopped Store, or --help");
            }
            Properties properties = configuration(Path.of(arguments.getOrDefault("--config", "conf")));
            String data = arguments.getOrDefault("--data-path", System.getProperty("app.data-path",
                    properties.getProperty("app.data-path")));
            if (data == null || data.isBlank()) {
                throw new IllegalArgumentException("Missing app.data-path; supply the effective --data-path");
            }
            if (data.contains("${")) {
                throw new IllegalArgumentException("Unresolved app.data-path; supply the effective --data-path");
            }
            Map<String, Object> options = new HashMap<>();
            properties.forEach((key, value) -> {
                if (key.toString().startsWith("rocksdb.")) {
                    options.put(key.toString(), value.toString());
                }
            });
            System.getProperties().forEach((key, value) -> {
                if (key.toString().startsWith("rocksdb.")) {
                    options.put(key.toString(), value.toString());
                }
            });
            RocksDBRuntime.verify(String.valueOf(options.getOrDefault("rocksdb.provider", "rocksdb")));
            LegacyGraphIdChecker.InspectionResult result =
                    LegacyGraphIdChecker.checkOffline(data, LegacyGraphIdChecker.configuration(options));
            System.out.printf("Inspected %d databases, %d latest snapshots, metadata present: %s%n",
                              result.databases(), result.snapshots(), result.metadata());
            if (result.databases() == 0 && result.snapshots() == 0) {
                System.out.println("No business databases or snapshots were inspected; verify that the data path is correct.");
            } else {
                System.out.println("No legacy GraphID prefix or invalid GraphID mapping found in persisted " +
                                   "databases and latest snapshots: " + data);
            }
            System.out.println("This read-only check does not certify historical ownership or backup completeness. " +
                               "See docs/graphid-upgrade.md before upgrading.");
            return 0;
        } catch (Exception e) {
            System.err.println("GraphID check failed: " + e.getMessage());
            for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
                System.err.println("Caused by: " + cause.getMessage());
            }
            System.err.println("No data was converted. Preserve the complete 1.7 backup; see docs/graphid-upgrade.md.");
            return 2;
        }
    }

    private static Properties configuration(Path directory) {
        Path application = directory.resolve("application.yml");
        if (!Files.isRegularFile(application)) {
            throw new IllegalArgumentException("Missing Store configuration " + application);
        }
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        Path pd = directory.resolve("application-pd.yml");
        yaml.setResources(Files.isRegularFile(pd) ?
                          new FileSystemResource[]{new FileSystemResource(application), new FileSystemResource(pd)} :
                          new FileSystemResource[]{new FileSystemResource(application)});
        Properties properties = yaml.getObject();
        return properties == null ? new Properties() : properties;
    }

    private static void help() {
        System.out.println("""
                Usage: bin/hugegraph-store-check.sh --check [options]
                  --check            Inspect a stopped Store without changing its databases
                  --config DIR       Local application.yml/application-pd.yml (default: conf)
                  --data-path PATHS  Effective app.data-path; comma-separated roots are supported
                  --help             Show this help

                Run once on every stopped Store with its effective configuration, including RocksDB
                options. No PD, Server, or running Store is required. Exit 0 means no incompatible
                persisted GraphID was found; exit 2 means legacy data, a locked/unreadable database,
                or a configuration error. The diagnostic includes the affected path and table.

                The check cannot see another process's unflushed writes or prove that a backup is
                complete. Preserve matching 1.7 Store, PD and Server backups before upgrading.
                A failed check needs the recovery/upgrade procedure in docs/graphid-upgrade.md.
                Do not assign FFFE data to a graph or modify RocksDB keys by hand.
                """);
    }
}
