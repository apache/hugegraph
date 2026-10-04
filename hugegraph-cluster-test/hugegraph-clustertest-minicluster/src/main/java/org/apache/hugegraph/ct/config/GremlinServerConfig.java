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

package org.apache.hugegraph.ct.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

public final class GremlinServerConfig {

    private static final Pattern HOST_SETTING =
            Pattern.compile("^#?host\\s*:.*$");
    private static final Pattern PORT_SETTING =
            Pattern.compile("^#?port\\s*:.*$");

    private GremlinServerConfig() {
        throw new IllegalStateException("Utility class");
    }

    public static void update(Path configPath, String host, int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid Gremlin port: " + port);
        }

        try {
            List<String> lines = Files.readAllLines(configPath,
                                                    StandardCharsets.UTF_8);
            boolean hostUpdated = false;
            boolean portUpdated = false;
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                if (HOST_SETTING.matcher(line).matches()) {
                    lines.set(i, "host: " + host);
                    hostUpdated = true;
                } else if (PORT_SETTING.matcher(line).matches()) {
                    lines.set(i, "port: " + port);
                    portUpdated = true;
                }
            }

            if (!hostUpdated || !portUpdated) {
                throw new IllegalStateException(
                        "Missing host or port setting in " + configPath);
            }
            Files.write(configPath, lines, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to update Gremlin server config " + configPath, e);
        }
    }
}
