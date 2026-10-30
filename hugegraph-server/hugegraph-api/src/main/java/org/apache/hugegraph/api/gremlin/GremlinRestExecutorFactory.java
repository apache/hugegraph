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

package org.apache.hugegraph.api.gremlin;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

import org.apache.hugegraph.auth.StandardAuthenticator;
import org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.server.HugeGraphWsAndHttpChannelizer;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.util.ser.MessageTextSerializer;
import org.slf4j.Logger;

public final class GremlinRestExecutorFactory {

    private static final Logger LOG = Log.logger(GremlinRestExecutorFactory.class);

    private GremlinRestExecutorFactory() {
    }

    public static GremlinRestExecutor standalone(HugeConfig config) {
        return new ForwardGremlinRestExecutor(config);
    }

    public static GremlinRestExecutor create(HugeConfig config, GremlinServer server,
                                             String restConfigFile) throws IOException {
        Settings settings = server.getServerGremlinExecutor().getSettings();
        URI target = URI.create(config.get(ServerOptions.GREMLIN_SERVER_URL));
        if (!targetsLocalServer(target, settings)) {
            return new ForwardGremlinRestExecutor(config);
        }
        String unsupported = unsupportedConfiguration(config, settings, restConfigFile);
        if (unsupported != null) {
            LOG.warn("REST Gremlin uses HTTP forwarding: {}", unsupported);
            return new ForwardGremlinRestExecutor(config);
        }
        return new EmbeddedGremlinRestExecutor(config, server.getServerGremlinExecutor());
    }

    private static String unsupportedConfiguration(HugeConfig config, Settings settings,
                                                    String restConfigFile) {
        if (!HugeGraphWsAndHttpChannelizer.class.getName().equals(settings.channelizer)) {
            return "the Gremlin Channelizer does not support embedded execution";
        }
        if (settings.authorization != null && settings.authorization.authorizer != null) {
            return "a custom Gremlin authorizer requires listener-side authorization";
        }
        if (settings.authentication == null) {
            return "the Gremlin authentication configuration is unavailable";
        }
        String restAuth = config.get(ServerOptions.AUTHENTICATOR);
        String gremlinAuth = settings.authentication.authenticator;
        if (restAuth.isEmpty()) {
            return AllowAllAuthenticator.class.getName().equals(gremlinAuth) ? null :
                   "REST and Gremlin authentication do not match";
        }
        if (!StandardAuthenticator.class.getName().equals(restAuth) || !restAuth.equals(gremlinAuth) ||
            !WsAndHttpBasicAuthHandler.class.getName().equals(settings.authentication.authenticationHandler)) {
            return "embedded execution requires matching standard HugeGraph authentication";
        }
        Object tokens = settings.authentication.config == null ? null :
                        settings.authentication.config.get("tokens");
        if (!(tokens instanceof String)) {
            return "REST and Gremlin must share the same authentication config file";
        }
        try {
            return Path.of(restConfigFile).toRealPath().equals(Path.of((String) tokens).toRealPath()) ?
                   null : "REST and Gremlin use different authentication config files";
        } catch (IOException | InvalidPathException e) {
            return "the shared REST and Gremlin authentication config file cannot be resolved";
        }
    }

    private static boolean targetsLocalServer(URI target, Settings settings) throws IOException {
        String scheme = target.getScheme();
        if (!("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) ||
            target.getHost() == null || target.getUserInfo() != null || target.getFragment() != null) {
            return false;
        }
        int port = target.getPort() == -1 ?
                   ("https".equalsIgnoreCase(scheme) ? 443 : 80) : target.getPort();
        // A different local port may identify another Gremlin Server process.
        if (port != settings.port || settings.host == null || settings.host.isEmpty()) {
            return false;
        }
        try {
            return isSameProcessTarget(InetAddress.getAllByName(target.getHost()),
                                       InetAddress.getByName(settings.host));
        } catch (UnknownHostException ignored) {
            // Keep unresolved forwarding targets on the existing HTTP client path.
            return false;
        }
    }

    private static boolean isSameProcessTarget(InetAddress[] targetAddresses,
                                               InetAddress bindAddress) throws IOException {
        if (targetAddresses.length == 0 || !isLocalAddress(bindAddress)) {
            return false;
        }
        for (InetAddress address : targetAddresses) {
            if (address.isAnyLocalAddress()) {
                return false;
            }
            if (bindAddress.isAnyLocalAddress()) {
                // Do not assume an IPv6 wildcard listener accepts IPv4 (or vice versa).
                if (address.getClass() != bindAddress.getClass() || !isLocalAddress(address)) {
                    return false;
                }
            } else if (!bindAddress.equals(address)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isLocalAddress(InetAddress address) throws IOException {
        return address.isAnyLocalAddress() || address.isLoopbackAddress() ||
               NetworkInterface.getByInetAddress(address) != null;
    }

    public static void initialize(GremlinRestExecutor executor, GremlinServer server) {
        if (executor instanceof EmbeddedGremlinRestExecutor) {
            HugeGraphWsAndHttpChannelizer channelizer =
                    (HugeGraphWsAndHttpChannelizer) server.getChannelizer();
            MessageTextSerializer<?> serializer = channelizer.jsonSerializer();
            if (serializer == null) {
                LOG.warn("REST Gremlin uses HTTP forwarding: the initialized listener has no " +
                         "application/json text serializer");
            }
            ((EmbeddedGremlinRestExecutor) executor).initialize(serializer);
        }
    }
}
