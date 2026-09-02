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

package org.apache.hugegraph.ranger;

import java.net.InetAddress;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.StandardAuthManager;
import org.apache.hugegraph.auth.UserWithRole;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.masterelection.RoleElectionOptions;
import org.apache.hugegraph.rpc.RpcClientProviderWithAuth;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.core.SecurityContext;

/**
 * HugeAuthenticator implementation that delegates authorization decisions to
 * Apache Ranger while using the standard HugeGraph graph store for user
 * identity and token management.
 *
 * <p>Configure in rest-server.properties:
 * <pre>
 *   auth.authenticator=org.apache.hugegraph.ranger.RangerHugeGraphAuthenticator
 *   auth.ranger.service_name=hugegraph
 *   auth.ranger.config=/etc/ranger/hugegraph/ranger-hugegraph-security.xml
 * </pre>
 */
public class RangerHugeGraphAuthenticator implements HugeAuthenticator {

    private static final Logger LOG =
            LoggerFactory.getLogger(RangerHugeGraphAuthenticator.class);

    private static final String INITING_STORE = "initing_store";

    private HugeGraph graph;
    private RangerAuthManager rangerAuthManager;

    @Override
    public void setup(HugeConfig config) {
        // --- open the backing HugeGraph (same as StandardAuthenticator) ---
        String graphName = config.get(ServerOptions.AUTH_GRAPH_STORE);
        java.util.Map<String, String> graphConfs =
                ConfigUtil.scanGraphsDir(config.get(ServerOptions.GRAPHS));
        String graphPath = graphConfs.get(graphName);
        E.checkArgument(graphPath != null,
                        "Can't find graph '%s' in '%s' for auth storage; " +
                        "check '%s' in rest-server.properties",
                        graphName, ServerOptions.GRAPHS,
                        ServerOptions.AUTH_GRAPH_STORE.name());

        HugeConfig graphConfig = new HugeConfig(graphPath);
        if (config.getProperty(INITING_STORE) != null &&
            config.getBoolean(INITING_STORE)) {
            graphConfig.setProperty(CoreOptions.RAFT_MODE.name(), "false");
        }
        String raftGroupPeers = config.get(ServerOptions.RAFT_GROUP_PEERS);
        graphConfig.addProperty(ServerOptions.RAFT_GROUP_PEERS.name(), raftGroupPeers);
        transferRoleWorkerConfig(graphConfig, config);

        this.graph = (HugeGraph) GraphFactory.open(graphConfig);

        String remoteUrl = config.get(ServerOptions.AUTH_REMOTE_URL);
        if (org.apache.commons.lang.StringUtils.isNotEmpty(remoteUrl)) {
            RpcClientProviderWithAuth clientProvider =
                    new RpcClientProviderWithAuth(config);
            this.graph.switchAuthManager(clientProvider.authManager());
        }

        // --- wrap the standard AuthManager with a Ranger policy enforcer ---
        String serviceName = config.getProperty(RangerOptions.RANGER_SERVICE_NAME) != null
                ? (String) config.getProperty(RangerOptions.RANGER_SERVICE_NAME)
                : RangerOptions.DEFAULT_SERVICE_NAME;
        String rangerConfigFile = config.getProperty(RangerOptions.RANGER_CONFIG_FILE) != null
                ? (String) config.getProperty(RangerOptions.RANGER_CONFIG_FILE)
                : null;

        // graph.authManager() here returns AuthManagerProxy(StandardAuthManager).
        // We capture it as the delegate for RangerAuthManager. We do NOT call
        // graph.switchAuthManager() — that would require admin context and create a
        // delegation cycle. Instead we return rangerAuthManager directly from
        // authManager() below, bypassing the proxy chain for callers that go through
        // the authenticator (GraphManager.authManager → authenticator.authManager()).
        //
        // AuthManagerProxy.validateUser() sets a temporary admin context before
        // calling into StandardAuthManager, so findUser() / matchUser() work correctly.
        // No cycle: rangerAuthManager.delegate = AuthManagerProxy;
        // AuthManagerProxy.authManager = StandardAuthManager (never replaced).
        AuthManager delegateProxy = this.graph.authManager();
        LOG.info("RangerAuthManager delegate: {}", delegateProxy.getClass().getName());
        this.rangerAuthManager = new RangerAuthManager(
                delegateProxy, serviceName, rangerConfigFile);

        // Register the Ranger plugin so every per-request permission check in
        // HugeGraphAuthProxy also consults real Ranger policy, not just the
        // wildcard grant fetched once at login. This can only narrow access,
        // never widen it, and is a no-op for non-Ranger deployments.
        HugeGraphAuthProxy.setResourceAuthorizer(this.rangerAuthManager.rangerPlugin());

        LOG.info("RangerHugeGraphAuthenticator initialised, Ranger service={}",
                 serviceName);
    }

    @Override
    public UserWithRole authenticate(String username, String password, String token) {
        UserWithRole userWithRole = rangerAuthManager.authenticate(username, password, token);
        // admin always gets full admin role (same behaviour as StandardAuthenticator)
        if (USER_ADMIN.equals(userWithRole.username())) {
            return new UserWithRole(userWithRole.userId(), userWithRole.username(), ROLE_ADMIN);
        }
        return userWithRole;
    }

    @Override
    public void unauthorize(SecurityContext context) {
        HugeGraphAuthProxy.resetContext();
    }

    @Override
    public AuthManager authManager() {
        // Return rangerAuthManager directly. GraphManager.authManager() calls this,
        // so all auth decisions go through Ranger. The delegate inside RangerAuthManager
        // is AuthManagerProxy(StandardAuthManager) — not replaced via switchAuthManager,
        // so there is no delegation cycle.
        return this.rangerAuthManager;
    }

    @Override
    public HugeGraph graph() {
        E.checkState(this.graph != null, "Must call setup() before graph()");
        return this.graph;
    }

    @Override
    public void initAdminUser(String password) {
        String caller = Thread.currentThread().getName();
        E.checkState("main".equals(caller), "Invalid caller '%s'", caller);

        AuthManager authManager = this.graph().hugegraph().authManager();
        if (StandardAuthManager.isLocal(authManager) &&
            authManager.findUser(USER_ADMIN) == null) {
            org.apache.hugegraph.auth.HugeUser admin =
                    new org.apache.hugegraph.auth.HugeUser(USER_ADMIN);
            admin.password(
                    org.apache.hugegraph.util.StringEncoding.hashPassword(password));
            admin.creator(USER_SYSTEM);
            authManager.createUser(admin);
        }
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress remoteAddress) {
        return new PlainSaslNegotiator();
    }

    private void transferRoleWorkerConfig(HugeConfig graphConfig,
                                          HugeConfig serverConfig) {
        graphConfig.addProperty(
                RoleElectionOptions.NODE_EXTERNAL_URL.name(),
                serverConfig.get(ServerOptions.REST_SERVER_URL));
        graphConfig.addProperty(
                RoleElectionOptions.BASE_TIMEOUT_MILLISECOND.name(),
                serverConfig.get(RoleElectionOptions.BASE_TIMEOUT_MILLISECOND));
        graphConfig.addProperty(
                RoleElectionOptions.EXCEEDS_FAIL_COUNT.name(),
                serverConfig.get(RoleElectionOptions.EXCEEDS_FAIL_COUNT));
        graphConfig.addProperty(
                RoleElectionOptions.RANDOM_TIMEOUT_MILLISECOND.name(),
                serverConfig.get(RoleElectionOptions.RANDOM_TIMEOUT_MILLISECOND));
        graphConfig.addProperty(
                RoleElectionOptions.HEARTBEAT_INTERVAL_SECOND.name(),
                serverConfig.get(RoleElectionOptions.HEARTBEAT_INTERVAL_SECOND));
        graphConfig.addProperty(
                RoleElectionOptions.MASTER_DEAD_TIMES.name(),
                serverConfig.get(RoleElectionOptions.MASTER_DEAD_TIMES));
    }

    // -------------------------------------------------------------------------
    // Inner: minimal SASL PLAIN negotiator (identical to StandardAuthenticator)
    // -------------------------------------------------------------------------

    private class PlainSaslNegotiator implements SaslNegotiator {

        private static final byte NUL = 0;

        private String username;
        private String password;
        private String token;

        @Override
        public byte[] evaluateResponse(byte[] clientResponse)
                throws AuthenticationException {
            decode(clientResponse);
            return null;
        }

        @Override
        public boolean isComplete() {
            return this.username != null;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser()
                throws AuthenticationException {
            if (!isComplete()) {
                throw new AuthenticationException(
                        "SASL negotiation not complete.");
            }
            java.util.Map<String, String> creds = new java.util.HashMap<>(4);
            creds.put(KEY_USERNAME, username);
            creds.put(KEY_PASSWORD, password);
            creds.put(KEY_TOKEN, token);
            return authenticate(creds);
        }

        private void decode(byte[] bytes) throws AuthenticationException {
            this.username = null;
            this.password = null;
            int end = bytes.length;
            for (int i = bytes.length - 1; i >= 0; i--) {
                if (bytes[i] != NUL) {
                    continue;
                }
                if (this.password == null) {
                    password = new String(java.util.Arrays.copyOfRange(bytes, i + 1, end),
                                         java.nio.charset.StandardCharsets.UTF_8);
                } else if (this.username == null) {
                    username = new String(java.util.Arrays.copyOfRange(bytes, i + 1, end),
                                         java.nio.charset.StandardCharsets.UTF_8);
                }
                end = i;
            }
            if (this.username == null) {
                throw new AuthenticationException(
                        "SASL authentication ID must not be null.");
            }
            if (password.isEmpty()) {
                token = username;
            }
        }
    }
}
