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

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import com.sun.tools.attach.VirtualMachine;
import com.taobao.arthas.core.config.FeatureCodec;

class ArthasAttach {
    public static void main(String[] args) throws Exception {
        String ip = args[5];
        InetAddress address = InetAddress.getByName(ip);
        // Prefer native IPv4 loopback access for wildcard listeners, including dual-stack ::.
        String connectIp = address.isAnyLocalAddress() ? "127.0.0.1" : ip;
        Files.writeString(Path.of(args[4]).resolveSibling("connect-ip"), connectIp);
        Path home = Path.of(args[1]);
        Path top = Path.of(args[3]);
        VirtualMachine vm = VirtualMachine.attach(args[0]);
        Path configFile = null;
        try {
            Properties target = vm.getSystemProperties();
            String command = target.getProperty("sun.java.command", "");
            String main = "org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap";
            boolean server = Files.isRegularFile(top.resolve("bin/start-hugegraph.sh")) &&
                             (command.equals(main) || command.startsWith(main + " "));
            boolean store = Files.isRegularFile(top.resolve("bin/start-hugegraph-store.sh")) &&
                            (command.matches(Pattern.quote(top.resolve("lib/hg-store-node-").toString()) +
                                             "[^/]+[.]jar(?: .*|$)") ||
                             (command.matches("org[.]springframework[.]boot[.]loader[.]JarLauncher(?: .*|$)") &&
                              "HugeGraphStore".equals(target.getProperty("name"))));
            if (!server && !store) {
                throw new IllegalArgumentException("PID does not identify a Server or Store in this distribution");
            }
            // Allow exit cleanup only after this distribution's target has been validated.
            Files.createFile(Path.of(args[4]));
            // FeatureCodec drops blank values; a properties file preserves them.
            configFile = Files.createTempFile("hugegraph-arthas-", ".properties");
            Properties settings = new Properties();
            settings.setProperty("arthas.config.overrideAll", "true");
            settings.setProperty("arthas.ip", ip);
            settings.setProperty("arthas.telnetPort", args[2]);
            settings.setProperty("arthas.httpPort", "-1");
            settings.setProperty("arthas.mcpEndpoint", "");
            settings.setProperty("arthas.tunnelServer", "");
            settings.setProperty("arthas.disabledCommands", "");
            settings.setProperty("arthas.sessionTimeout", "1800");
            if (!args[6].isEmpty() || !args[7].isEmpty()) {
                settings.setProperty("arthas.username", args[6].isEmpty() ? "arthas" : args[6]);
            }
            if (!args[7].isEmpty()) {
                settings.setProperty("arthas.password", args[7]);
            }
            settings.setProperty("arthas.localConnectionNonAuth",
                                 target.getProperty("arthas.localConnectionNonAuth", "true"));
            try (OutputStream output = Files.newOutputStream(configFile)) {
                settings.store(output, null);
            }
            Map<String, String> config = new LinkedHashMap<>();
            config.put("config.overrideAll", "true");
            config.put("config.location", configFile.toString());
            config.put("home", home.toString());
            String agentArgs = home.resolve("arthas-core.jar") + ";" +
                               FeatureCodec.DEFAULT_COMMANDLINE_CODEC.toString(config);
            vm.loadAgent(home.resolve("arthas-agent.jar").toString(),
                         URLEncoder.encode(agentArgs, StandardCharsets.UTF_8));
        } finally {
            vm.detach();
            if (configFile != null) {
                Files.deleteIfExists(configFile);
            }
        }
    }
}
