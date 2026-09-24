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

package org.apache.hugegraph.api.filter;

import static org.apache.hugegraph.config.ServerOptions.WHITE_IP_STATUS;

import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeAuthenticator.User;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.utils.Charsets;
import org.gridkit.jvmtool.cmd.AntPathMatcher;
import org.slf4j.Logger;

import com.alipay.remoting.util.StringUtils;
import com.google.common.collect.ImmutableSet;

import jakarta.annotation.Priority;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.Provider;

@Provider
@PreMatching
@Priority(Priorities.AUTHENTICATION)
public class AuthenticationFilter implements ContainerRequestFilter, ContainerResponseFilter {

    public static final String BASIC_AUTH_PREFIX = "Basic ";
    public static final String BEARER_TOKEN_PREFIX = "Bearer ";

    public static final String ALL_GRAPH_SPACES = "*";

    private static final Logger LOG = Log.logger(AuthenticationFilter.class);

    private static final AntPathMatcher MATCHER = new AntPathMatcher();
    private static final Set<String> FIXED_WHITE_API_SET = ImmutableSet.of(
            "versions",
            "openapi.json"
    );
    /** Remove auth/login API from whitelist */
    private static final Set<String> FLEXIBLE_WHITE_API_SET = ImmutableSet.of();

    private static Boolean enabledWhiteIpCheck;
    private static final String STRING_WHITE_IP_LIST = "whiteiplist";
    private static final String STRING_ENABLE = "enable";

    @Context
    private jakarta.inject.Provider<GraphManager> managerProvider;

    @Context
    private jakarta.inject.Provider<Request> requestProvider;

    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;

    @Override
    public void filter(ContainerRequestContext context) throws IOException {
        if (isWhiteAPI(context)) {
            return;
        }
        GraphManager manager = this.managerProvider.get();
        User user = this.authenticate(context);

        // Inject request graph space into AuthContext for permission check
        // Extract graphspace from path like: /graphspaces/{graphspace}/...
        String path = context.getUriInfo().getPath();
        LOG.debug("AuthenticationFilter: path={}", path);
        if (path != null && path.contains("graphspaces/")) {
            String[] parts = path.split("/");
            for (int i = 0; i < parts.length - 1; i++) {
                if ("graphspaces".equals(parts[i]) && i + 1 < parts.length) {
                    String requestGraphSpace = parts[i + 1];
                    HugeGraphAuthProxy.setRequestGraphSpace(requestGraphSpace);
                    LOG.debug("AuthenticationFilter: set RequestGraphSpace={}", requestGraphSpace);
                    break;
                }
            }
        }

        Authorizer authorizer = new Authorizer(manager, user, context.getUriInfo());
        context.setSecurityContext(authorizer);
    }

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) throws IOException {
        // Clean up ThreadLocal variables after request is processed
        // This prevents memory leaks in thread pool
        HugeGraphAuthProxy.resetSpaceContext();
        LOG.debug("HugeGraphAuthProxy ThreadLocal cleaned up after request");
    }

    protected User authenticate(ContainerRequestContext context) {
        GraphManager manager = this.managerProvider.get();
        E.checkState(manager != null, "Context GraphManager is absent");

        if (!manager.requireAuthentication()) {
            // Return anonymous user with an admin role if disable authentication
            return User.ANONYMOUS;
        }

        // Get peer info
        Request request = this.requestProvider.get();
        String peer = null;
        String path = null;
        if (request != null) {
            peer = request.getRemoteAddr() + ":" + request.getRemotePort();
            path = request.getRequestURI();
        }

        // Check whiteIp
        if (enabledWhiteIpCheck == null) {
            String whiteIpStatus = this.configProvider.get().get(WHITE_IP_STATUS);
            enabledWhiteIpCheck = Objects.equals(whiteIpStatus, STRING_ENABLE);
        }

        if (enabledWhiteIpCheck && request != null) {
            peer = request.getRemoteAddr() + ":" + request.getRemotePort();
            path = request.getRequestURI();

            String remoteIp = request.getRemoteAddr();
            Set<String> whiteIpList = manager.authManager().listWhiteIPs();
            boolean whiteIpEnabled = manager.authManager().getWhiteIpStatus();
            if (!path.contains(STRING_WHITE_IP_LIST) && whiteIpEnabled &&
                !whiteIpList.contains(remoteIp)) {
                throw new ForbiddenException(String.format("Remote ip '%s' is not permitted",
                                                           remoteIp));
            }
        }

        Map<String, String> credentials = new HashMap<>();
        // Extract authentication credentials
        String auth = context.getHeaderString(HttpHeaders.AUTHORIZATION);
        if (auth == null) {
            throw new NotAuthorizedException("Authentication credentials are required",
                                             "Missing authentication credentials");
        }

        if (auth.startsWith(BASIC_AUTH_PREFIX)) {
            auth = auth.substring(BASIC_AUTH_PREFIX.length());
            auth = new String(DatatypeConverter.parseBase64Binary(auth), Charsets.ASCII_CHARSET);
            String[] values = auth.split(":");
            if (values.length != 2) {
                throw new BadRequestException("Invalid syntax for username and password");
            }

            final String username = values[0];
            final String password = values[1];

            if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
                throw new BadRequestException("Invalid syntax for username and password");
            }

            credentials.put(HugeAuthenticator.KEY_USERNAME, username);
            credentials.put(HugeAuthenticator.KEY_PASSWORD, password);
        } else if (auth.startsWith(BEARER_TOKEN_PREFIX)) {
            String token = auth.substring(BEARER_TOKEN_PREFIX.length());
            credentials.put(HugeAuthenticator.KEY_TOKEN, token);
        } else {
            throw new BadRequestException("Only HTTP Basic or Bearer authentication is supported");
        }

        credentials.put(HugeAuthenticator.KEY_ADDRESS, peer);
        credentials.put(HugeAuthenticator.KEY_PATH, path);

        // Validate the extracted credentials
        try {
            return manager.authenticate(credentials);
        } catch (AuthenticationException e) {
            throw new NotAuthorizedException("Authentication failed", e.getMessage());
        }
    }

    public static class Authorizer implements SecurityContext {

        private final UriInfo uri;
        private final User user;
        private final Principal principal;
        private final GraphManager manager;

        public Authorizer(GraphManager manager, final User user, final UriInfo uri) {
            E.checkNotNull(user, "user");
            E.checkNotNull(uri, "uri");
            this.manager = manager;
            this.uri = uri;
            this.user = user;
            this.principal = new UserPrincipal();
        }

        public String username() {
            return this.user.username();
        }

        public RolePermission role() {
            return this.user.role();
        }

        @Override
        public Principal getUserPrincipal() {
            return this.principal;
        }

        @Override
        public boolean isUserInRole(String required) {
            if (required.equals(HugeAuthenticator.KEY_DYNAMIC)) {
                // Let the resource itself determine dynamically
                return true;
            } else {
                return this.matchPermission(required);
            }
        }

        @Override
        public boolean isSecure() {
            return "https".equals(this.uri.getRequestUri().getScheme());
        }

        @Override
        public String getAuthenticationScheme() {
            return SecurityContext.BASIC_AUTH;
        }

        private boolean matchPermission(String required) {
            boolean valid;
            HugeAuthenticator.RequiredPerm requiredPerm;

            /*
             * if request url contains graph space and the corresponding space
             * does not enable permission check, return true
             * */
            if (!isAuth()) {
                return true;
            }

            if (!required.startsWith(HugeAuthenticator.KEY_GRAPHSPACE)) {
                // Permission format like: "admin", "space", "analyst", "space_member"
                requiredPerm = new HugeAuthenticator.RequiredPerm();
                requiredPerm.owner(required);

                // For space-level roles, set graphSpace from path parameter
                if ("space".equals(required) || "space_member".equals(required)) {
                    // If graphspace parameter is not in path, use DEFAULT
                    List<String> graphSpaceParams = this.uri.getPathParameters().get("graphspace");
                    String graphSpace = "DEFAULT";
                    if (graphSpaceParams != null && !graphSpaceParams.isEmpty()) {
                        graphSpace = graphSpaceParams.get(0);
                    }
                    requiredPerm.graphSpace(graphSpace);
                }

                // Role inheritance is handled in HugeAuthenticator.matchSpace()
                valid = HugeAuthenticator.RolePerm.matchApiRequiredPerm(this.role(), requiredPerm);
            } else {
                // The required like:
                // $graphspace=graphspace $owner=graph1 $action=vertex_write
                requiredPerm = HugeAuthenticator.RequiredPerm.fromPermission(required);

                /*
                 * Replace graphspace value (it may be a variable) if the
                 * permission format like:
                 * "$graphspace=$graphspace $owner=$graph $action=vertex_write"
                 */
                String graphSpace = requiredPerm.graphSpace();
                if (graphSpace.startsWith(HugeAuthenticator.VAR_PREFIX)) {
                    int prefixLen = HugeAuthenticator.VAR_PREFIX.length();
                    assert graphSpace.length() > prefixLen;
                    graphSpace = graphSpace.substring(prefixLen);
                    graphSpace = this.getPathParameter(graphSpace);
                    requiredPerm.graphSpace(graphSpace);
                }

                /*
                 * Replace owner value(it may be a variable) if the permission
                 * format like: "$graphspace=$graphspace $owner=$graph $action=vertex_write"
                 */
                String owner = requiredPerm.owner();
                if (owner.startsWith(HugeAuthenticator.VAR_PREFIX)) {
                    // Replace `$graph` with graph name like "graph1"
                    int prefixLen = HugeAuthenticator.VAR_PREFIX.length();
                    assert owner.length() > prefixLen;
                    owner = owner.substring(prefixLen);
                    owner = this.getPathParameter(owner);
                    requiredPerm.owner(owner);
                }
                valid = HugeAuthenticator.RolePerm.matchApiRequiredPerm(this.role(), requiredPerm);
            }

            if (!valid &&
                !required.equals(HugeAuthenticator.USER_ADMIN)) {
                LOG.info(
                        "Permission denied for user '{}', action '{}', resource '{}'",
                        user.userId().asString(),
                        requiredPerm.action().string(),
                        requiredPerm.resourceObject());
            }
            return valid;
        }

        private String getPathParameter(String key) {
            List<String> params = this.uri.getPathParameters().get(key);
            // For graphspace parameter, use "DEFAULT" if not present in path
            if ("graphspace".equals(key) && (params == null || params.isEmpty())) {
                return "DEFAULT";
            }
            E.checkState(params != null && params.size() == 1,
                         "There is no matched path parameter: '%s'", key);
            return params.get(0);
        }

        private boolean isAuth() {
            List<String> params = this.uri.getPathParameters().get(
                    "graphspace");
            if (params != null && params.size() == 1) {
                String graphSpace = params.get(0);
                if (ALL_GRAPH_SPACES.equals(graphSpace)) {
                    return true;
                }
                E.checkArgumentNotNull(this.manager.graphSpace(graphSpace),
                                       "The graph space '%s' does not exist",
                                       graphSpace);
                return this.manager.graphSpace(graphSpace).auth();
            } else {
                return true;
            }
        }

        private final class UserPrincipal implements Principal {

            @Override
            public String getName() {
                return Authorizer.this.user.getName();
            }

            @Override
            public String toString() {
                return Authorizer.this.user.toString();
            }

            @Override
            public int hashCode() {
                return Authorizer.this.user.hashCode();
            }

            @Override
            public boolean equals(Object obj) {
                return Authorizer.this.user.equals(obj);
            }
        }
    }

    public static boolean isWhiteAPI(ContainerRequestContext context) {
        String path = context.getUriInfo().getPath();
        if (FIXED_WHITE_API_SET.contains(path)) {
            return true;
        }

        for (String whiteApi : FLEXIBLE_WHITE_API_SET) {
            if (MATCHER.match(whiteApi, path)) {
                return true;
            }
        }
        return false;
    }
}
