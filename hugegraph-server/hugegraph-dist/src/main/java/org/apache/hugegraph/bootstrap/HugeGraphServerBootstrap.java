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

package org.apache.hugegraph.bootstrap;

import java.io.FileWriter;
import java.io.IOException;
import java.security.Security;

import org.apache.hugegraph.dist.HugeGraphServer;
import org.apache.hugegraph.security.HugeSecurityManager;

public final class HugeGraphServerBootstrap {

    private HugeGraphServerBootstrap() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 ||
            !("true".equals(args[0]) || "false".equals(args[0]))) {
            System.err.println("ERROR: Expected literal validation flag");
            System.exit(1);
            return;
        }

        if (Boolean.parseBoolean(args[0])) {
            try {
                validateDnsCacheTtl(
                        Security.getProperty("networkaddress.cache.ttl"));
            } catch (Throwable e) {
                reportFatal("ERROR: Java security property " +
                            "networkaddress.cache.ttl must load as a " +
                            "finite positive integer " +
                            "(java.security.properties=" +
                            System.getProperty("java.security.properties") +
                            "): " + e);
                System.exit(1);
                return;
            }

            try {
                System.setSecurityManager(new HugeSecurityManager());
                if (!(System.getSecurityManager() instanceof
                      HugeSecurityManager)) {
                    throw new IllegalStateException(
                            "Unexpected security manager");
                }
            } catch (Throwable e) {
                reportFatal("ERROR: Failed to install " +
                            "HugeSecurityManager: " + e);
                System.exit(1);
                return;
            }
        }

        if (args.length != 3) {
            System.err.println("ERROR: Expected validation flag and two " +
                               "HugeGraphServer configuration paths");
            System.exit(1);
            return;
        }

        HugeGraphServer.main(new String[]{args[1], args[2]});
    }

    static void validateDnsCacheTtl(String value) {
        int ttl = Integer.parseInt(value);
        if (ttl <= 0) {
            throw new IllegalArgumentException("DNS cache TTL must be positive");
        }
    }

    /**
     * In daemon mode the launcher redirects stderr to the stdout log, so a
     * fatal error printed only there never reaches the server log that
     * start-hugegraph.sh points operators at. The launcher passes that log's
     * path so the cause can be appended to both. This runs before the
     * security manager is installed and must not touch the logging framework:
     * the DNS policy being validated here is the very policy logging-time
     * hostname resolution would cache.
     */
    private static void reportFatal(String message) {
        System.err.println(message);
        String errorLog = System.getProperty("hugegraph.bootstrap.error.log");
        if (errorLog == null || errorLog.isEmpty()) {
            return;
        }
        try (FileWriter writer = new FileWriter(errorLog, true)) {
            writer.write(message + System.lineSeparator());
        } catch (IOException ignored) {
            // Best effort only: stderr already carries the message
        }
    }
}
