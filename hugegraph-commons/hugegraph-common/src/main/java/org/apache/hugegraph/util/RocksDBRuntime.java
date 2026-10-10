/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.util;

import java.net.URL;
import java.security.CodeSource;

/** Match configuration to the JNI Java classes selected by the launcher. */
public final class RocksDBRuntime {

    private RocksDBRuntime() {
    }

    private static URL origin(Class<?> runtime) {
        CodeSource source = runtime.getProtectionDomain().getCodeSource();
        if (source == null || source.getLocation() == null) {
            throw new IllegalStateException("Cannot verify RocksDB runtime origin for " + runtime.getName() +
                                            "; use a class loader with a CodeSource location");
        }
        return source.getLocation();
    }

    public static void verify(String configuredProvider) {
        if (!"rocksdb".equals(configuredProvider) && !"topling".equals(configuredProvider)) {
            throw new IllegalArgumentException("Invalid rocksdb.provider: " + configuredProvider);
        }
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> rocks;
            try {
                rocks = Class.forName("org.rocksdb.RocksDB", false, loader);
            } catch (ClassNotFoundException contextLoaderMiss) {
                loader = RocksDBRuntime.class.getClassLoader();
                rocks = Class.forName("org.rocksdb.RocksDB", false, loader);
            }
            URL origin = origin(rocks);
            boolean topling = false;
            try {
                Class<?> marker = Class.forName("org.rocksdb.SidePluginRepo", false, loader);
                topling = origin.equals(origin(marker));
            } catch (ClassNotFoundException standardRuntime) {
                // Standard RocksDB has no Topling marker class.
            }
            if (topling != "topling".equals(configuredProvider)) {
                throw new IllegalStateException("Configured rocksdb.provider=" + configuredProvider +
                                                " conflicts with selected RocksDB Java runtime: " + origin +
                                                "; match the launcher opt-in and business configuration");
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("RocksDB Java runtime is missing", e);
        }
    }
}
