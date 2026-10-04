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

package org.apache.hugegraph.ct.base;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

public class EnvUtil {

    private static final Logger LOG = HGTestLogger.UTIL_LOG;
    private static final int PORT_CONNECT_TIMEOUT_MILLIS = 500;
    private static final Set<Integer> ports = new HashSet<>();

    public static int getAvailablePort() {
        try {
            int port = -1;
            while (port < 0 || ports.contains(port)) {
                ServerSocket socket = new ServerSocket(0);
                port = socket.getLocalPort();
                socket.close();
            }
            ports.add(port);
            return port;
        } catch (IOException e) {
            LOG.error("Failed to get available ports", e);
            return -1;
        }
    }

    public static boolean isPortOpen(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port),
                           PORT_CONNECT_TIMEOUT_MILLIS);
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    public static void copyFileToDestination(Path source, Path destination) {
        try {
            ensureParentDirectoryExists(destination);
            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ioException) {
            LOG.error("Failed to copy files to destination dir", ioException);
            throw new RuntimeException(ioException);
        }
    }

    private static void ensureParentDirectoryExists(Path destination) throws IOException {
        Path parentDir = destination.getParent();
        if (parentDir != null && Files.notExists(parentDir)) {
            Files.createDirectories(parentDir);
        }
    }
}
