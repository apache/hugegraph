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

package org.apache.hugegraph.auth;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.HugeGraphAuthProxy.Context;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.type.Nameable;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

import jakarta.ws.rs.core.SecurityContext;

public interface HugeAuthenticator extends Authenticator {

    String KEY_USERNAME = CredentialGraphTokens.PROPERTY_USERNAME;
    String KEY_PASSWORD = CredentialGraphTokens.PROPERTY_PASSWORD;
    String KEY_TOKEN = "token";
    String KEY_ROLE = "role";
    String KEY_ADDRESS = "address";
    String KEY_PATH = "path";
    String GENERAL_PATTERN = "*";

    String USER_SYSTEM = "system";
    String USER_ADMIN = "admin";
    String USER_ANONY = AuthenticatedUser.ANONYMOUS_USERNAME;

    RolePermission ROLE_NONE = RolePermission.none();
    RolePermission ROLE_ADMIN = RolePermission.admin();

    String VAR_PREFIX = "$";
    String KEY_GRAPHSPACE = VAR_PREFIX + "graphspace";
    String KEY_OWNER = VAR_PREFIX + "owner";
    String KEY_DYNAMIC = VAR_PREFIX + "dynamic";
    String KEY_ACTION = VAR_PREFIX + "action";

    void setup(HugeConfig config);

    UserWithRole authenticate(String username, String password, String token);

    void unauthorize(SecurityContext context);

    AuthManager authManager();

    HugeGraph graph();

    @Override
    default void setup(final Map<String, Object> config) {
        E.checkState(config != null,
                     "Must provide a 'config' in the 'authentication'");
        String path = (String) config.get("tokens");
        E.checkState(path != null,
                     "Credentials configuration missing key 'tokens'");
        OptionSpace.register("tokens", ServerOptions.instance());
        this.setup(new HugeConfig(path));
    }

    @Override
    default User authenticate(final Map<String, String> credentials)
            throws AuthenticationException {

        HugeGraphAuthProxy.resetContext();

        User user = User.ANONYMOUS;
        if (this.requireAuthentication()) {
            String username = credentials.get(KEY_USERNAME);
            String password = credentials.get(KEY_PASSWORD);
            String token = credentials.get(KEY_TOKEN);

            UserWithRole role = this.authenticate(username, password, token);
            if (!verifyRole(role.role())) {
                // Throw if not certified
                String message = "Incorrect username or password";
                throw new AuthenticationException(message);
            }
            user = new User(role.username(), role.role());
            user.client(credentials.get(KEY_ADDRESS));
        }

        HugeGraphAuthProxy.logUser(user, credentials.get(KEY_PATH));
        // TODO: Ensure context lifecycle in GraphServer & AuthServer(#AccessLogFilter)
        HugeGraphAuthProxy.setContext(new Context(user));

        return user;
    }

    @Override
    default boolean requireAuthentication() {
        return true;
    }

    default boolean verifyRole(RolePermission role) {
        return role != ROLE_NONE && role != null;
    }

    void initAdminUser(String password) throws Exception;

    static HugeAuthenticator loadAuthenticator(HugeConfig conf) {
        String authClass = conf.get(ServerOptions.AUTHENTICATOR);
        if (authClass.isEmpty()) {
            return null;
        }

        HugeAuthenticator authenticator;
        ClassLoader cl = conf.getClass().getClassLoader();
        try {
            authenticator = (HugeAuthenticator) cl.loadClass(authClass)
                                                  .getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new HugeException("Failed to load authenticator: '%s'", authClass, e);
        }

        authenticator.setup(conf);

        return authenticator;
    }

    class User extends AuthenticatedUser {

        public static final User ADMIN = new User(USER_ADMIN, ROLE_ADMIN);
        public static final User ANONYMOUS = new User(USER_ANONY, ROLE_ADMIN);

        private final RolePermission role;
        private final Id userId;
        private String client; // peer

        public User(String username, RolePermission role) {
            super(username);
            E.checkNotNull(username, "username");
            E.checkNotNull(role, "role");
            this.role = role;
            this.client = null;
            /*
             * 1. Use username as the id to simplify getting userId
             * 2. Only used as cache's key in auth proxy now
             */
            this.userId = IdGenerator.of(username);
        }

        public String username() {
            return this.getName();
        }

        public Id userId() {
            return this.userId;
        }

        public RolePermission role() {
            return this.role;
        }

        public void client(String client) {
            this.client = client;
        }

        public String client() {
            return client;
        }

        @Override
        public boolean isAnonymous() {
            return this == ANONYMOUS || this == ANONYMOUS_USER;
        }

        @Override
        public int hashCode() {
            return this.username().hashCode() ^ this.role().hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof User)) {
                return false;
            }

            User other = (User) object;
            return this.username().equals(other.username()) &&
                   this.role().equals(other.role());
        }

        @Override
        public String toString() {
            return String.format("User{username=%s,role=%s}",
                                 this.username(), this.role());
        }

        public String toJson() {
            UserJson json = new UserJson();
            json.username = this.username();
            json.role = this.role();
            json.client = this.client();
            return JsonUtil.toJson(json);
        }

        public static User fromJson(String json) {
            if (json == null) {
                return null;
            }
            UserJson userJson = JsonUtil.fromJson(json, UserJson.class);
            if (userJson != null) {
                User user = new User(userJson.username,
                                     RolePermission.builtin(userJson.role));
                user.client(userJson.client);
                return user;
            }
            return null;
        }

        public static class UserJson {

            @JsonProperty("username")
            private String username;
            @JsonProperty("role")
            private RolePermission role;
            @JsonProperty("client")
            private String client;
        }
    }

    class RolePerm {

        public static final String ANY = "*";
        public static final String POUND_SEPARATOR = "#";
        @JsonProperty("roles") // graphspace -> graph -> action -> resource
        private final Map<String, Map<String, Map<HugePermission, Object>>> roles;

        public RolePerm() {
            this.roles = new HashMap<>();
        }

        public RolePerm(Map<String, Map<String, Map<HugePermission,
                Object>>> roles) {
            this.roles = roles;
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }

        private static boolean matchedPrefix(String key, String graph) {
            if (key.equals(graph)) {
                return true;
            } else if (key.endsWith("*")) {
                key = key.substring(0, key.length() - 1);
                return graph.startsWith(key);
            }
            return false;
        }

        private static Object matchedAction(HugePermission action,
                                            Map<HugePermission, Object> perms) {
            Object matched = perms.get(action);
            if (matched != null) {
                return matched;
            }
            for (Map.Entry<HugePermission, Object> e : perms.entrySet()) {
                HugePermission permission = e.getKey();
                if (permission == HugePermission.SPACE ||
                    permission == HugePermission.SPACE_MEMBER) {
                    continue;
                }
                // Maybe required = ANY
                if (action.match(permission) ||
                    action.equals(HugePermission.EXECUTE)) {
                    // Return matched resource of corresponding action
                    return e.getValue();
                }
            }
            return null;
        }

        public static boolean matchApiRequiredPerm(Object role, RequiredPerm requiredPerm) {
            if (RolePermission.isAdmin((RolePermission) role)) {
                return true;
            }
            if (ROLE_NONE.equals(role)) {
                return false;
            }

            RolePerm rolePerm = RolePerm.fromJson(role);
            if (rolePerm.matchSpace(requiredPerm.graphSpace(), requiredPerm.owner)) {
                return true;
            }

            if (requiredPerm.action() == HugePermission.NONE) {
                // None action means any action is OK if the owner matched
                return rolePerm.matchOwner(requiredPerm.graphSpace(),
                                           requiredPerm.owner());
            }
            return rolePerm.matchResource(requiredPerm.action(),
                                          requiredPerm.resourceObject());
        }

        public static boolean match(Object role, HugePermission required,
                                    ResourceObject<?> resourceObject) {
            if (role == null || ROLE_NONE.equals(role)) {
                return false;
            }
            if (RolePermission.isAdmin((RolePermission) role)) {
                return true;
            }

            RolePerm rolePerm = RolePerm.fromJson(role);
            // Check if user is space manager（member cannot operate auth api)
            if (rolePerm.matchSpace(resourceObject.graphSpace(), "space")) {
                return true;
            }
            return rolePerm.matchResource(required, resourceObject);
        }

        public static boolean match(Object role, RolePermission grant,
                                    ResourceObject<?> resourceObject) {
            if (role == null || ROLE_NONE.equals(role)) {
                return false;
            }

            if (RolePermission.isAdmin((RolePermission) role)) {
                return true;
            }

            if (resourceObject != null) {
                SchemaDefine.AuthElement element =
                        (SchemaDefine.AuthElement) resourceObject.operated();
                if (element instanceof HugeUser &&
                    ((HugeUser) element).name().equals(USER_ADMIN)) {
                    // Can't access admin by other users
                    return false;
                }
            }

            RolePermission rolePerm = RolePermission.fromJson(role);
            return rolePerm.contains(grant);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public static RolePerm fromJson(Object role) {
            RolePermission table = RolePermission.fromJson(role);
            return new RolePerm((Map) table.map());
        }

        private boolean matchOwner(String graphSpace, String owner) {
            if (graphSpace == null && owner == null) {
                return true;
            }

            return this.roles.containsKey(graphSpace) &&
                   this.roles.get(graphSpace).containsKey(owner);
        }

        private boolean matchSpace(String graphSpace, String requiredRole) {
            if (graphSpace == null) {
                return true;
            }

            if (!this.roles.containsKey(graphSpace)) {
                return false;
            }

            Map<String, Map<HugePermission, Object>> graphPermissions =
                    this.roles.get(graphSpace);

            for (Map<HugePermission, Object> permissions : graphPermissions.values()) {
                if (permissions == null) {
                    continue;
                }

                if (permissions.containsKey(HugePermission.SPACE)) {
                    return true;
                }

                if ("space_member".equals(requiredRole) &&
                    permissions.containsKey(HugePermission.SPACE_MEMBER)) {
                    return true;
                }
            }

            return false;
        }

        private boolean matchResource(HugePermission requiredAction,
                                      ResourceObject<?> requiredResource) {
            E.checkNotNull(requiredResource, "resource object");

            /*
             * Is resource allowed to access by anyone?
             * TODO: only allowed resource of related type(USER/TASK/VAR),
             *       such as role VAR is allowed to access '~variables' label
             */
            if (HugeResource.allowed(requiredResource)) {
                return true;
            }

            Map<String, Map<HugePermission, Object>> innerRoles =
                    this.roles.get(requiredResource.graphSpace());
            if (innerRoles == null) {
                return false;
            }

            // * or {graph}
            String owner = requiredResource.graph();
            for (Map.Entry<String, Map<HugePermission, Object>> e : innerRoles.entrySet()) {
                if (!matchedPrefix(e.getKey(), owner)) {
                    continue;
                }
                Map<HugePermission, Object> permissions = e.getValue();
                if (permissions == null) {
                    permissions = innerRoles.get(GENERAL_PATTERN);
                    if (permissions == null) {
                        continue;
                    }
                }

                Object permission = matchedAction(requiredAction, permissions);
                if (permission == null) {
                    continue;
                }

                var ressMap = (Map<String, List<HugeResource>>) permission;

                ResourceType requiredType = requiredResource.type();
                for (Map.Entry<String, List<HugeResource>> entry : ressMap.entrySet()) {
                    String[] typeLabel = entry.getKey().split(POUND_SEPARATOR);
                    ResourceType type = ResourceType.valueOf(typeLabel[0]);
                    /* assert one type can match but not equal to other only
                     * when it is related to schema and data
                     */
                    if (!type.match(requiredType)) {
                        continue;
                    } else if (type != requiredType) {
                        return true;
                    }

                    // check label
                    String requiredLabel;
                    if (requiredType.isSchema()) {
                        requiredLabel = ((Nameable) requiredResource.operated()).name();
                    } else if (requiredType.isGraph()) {
                        if (requiredResource.operated() instanceof HugeElement) {
                            requiredLabel = ((HugeElement) requiredResource.operated()).label();
                        } else {
                            requiredLabel = ((Nameable) requiredResource.operated()).name();
                        }
                    } else {
                        return true;
                    }
                    String label = typeLabel[1];
                    if (!(ANY.equals(label) ||
                          "null".equals(label) || matchLabel(requiredLabel, label))) {
                        continue;
                    } else if (requiredType.isSchema()) {
                        return true;
                    }

                    // check properties
                    List<HugeResource> ress = ressMap.get(type + POUND_SEPARATOR + label);

                    for (HugeResource res : ress) {
                        if (res.filter(requiredResource)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        /**
         * Safely match a label pattern against the required label.
         * Prevents ReDoS attacks by using controlled wildcard matching instead of
         * arbitrary regex patterns.
         *
         * @param requiredLabel the label to match against
         * @param pattern the pattern (may contain * and ? wildcards)
         * @return true if the label matches the pattern
         */
        private static boolean matchLabel(String requiredLabel, String pattern) {
            // Use simple wildcard matching instead of arbitrary regex
            if (pattern.contains("*") || pattern.contains("?")) {
                // Convert pattern to safe regex: escape special chars, then convert wildcards
                String regex = Pattern.quote(pattern)
                                      .replace("\\*", ".*")
                                      .replace("\\?", ".");
                return Pattern.matches(regex, requiredLabel);
            }
            // Simple equality check if no wildcards
            return requiredLabel.equals(pattern);
        }
    }

    class RequiredPerm {

        @JsonProperty("graphspace")
        private String graphSpace;
        @JsonProperty("owner")
        private String owner;
        @JsonProperty("action")
        private HugePermission action;
        @JsonProperty("resource")
        private ResourceType resource;

        public RequiredPerm() {
            this.graphSpace = "";
            this.owner = "";
            this.action = HugePermission.NONE;
            this.resource = ResourceType.NONE;
        }

        public static RequiredPerm fromPermission(String permission) {
            // Permission format like: "$graphspace=$default $owner=$graph1 $action=vertex-write"
            RequiredPerm
                    requiredPerm = new RequiredPerm();
            String[] spaceAndOwnerAndAction = permission.split(" ");
            String[] spaceKV = spaceAndOwnerAndAction[0].split("=", 2);
            E.checkState(spaceKV.length == 2 && spaceKV[0].equals(KEY_GRAPHSPACE),
                         "Bad permission format: '%s'", permission);
            requiredPerm.graphSpace(spaceKV[1]);

            String[] ownerKV = spaceAndOwnerAndAction[1].split("=", 2);
            E.checkState(ownerKV.length == 2 && ownerKV[0].equals(KEY_OWNER),
                         "Bad permission format: '%s'", permission);
            requiredPerm.owner(ownerKV[1]);

            if (spaceAndOwnerAndAction.length == 2) {
                // Return owner if no action (means NONE)
                return requiredPerm;
            }

            E.checkState(spaceAndOwnerAndAction.length == 3,
                         "Bad permission format: '%s'", permission);
            String[] actionKV = spaceAndOwnerAndAction[2].split("=", 2);
            E.checkState(actionKV.length == 2,
                         "Bad permission format: '%s'", permission);
            E.checkState(actionKV[0].equals(StandardAuthenticator.KEY_ACTION),
                         "Bad permission format: '%s'", permission);
            requiredPerm.action(actionKV[1]);

            return requiredPerm;
        }

        public RequiredPerm graphSpace(String graphSpace) {
            this.graphSpace = graphSpace;
            return this;
        }

        public RequiredPerm owner(String owner) {
            this.owner = owner;
            return this;
        }

        public String owner() {
            return this.owner;
        }

        public RequiredPerm action(String action) {
            this.parseAction(action);
            return this;
        }

        public HugePermission action() {
            return this.action;
        }

        public ResourceType resource() {
            return this.resource;
        }

        public String graphSpace() {
            return this.graphSpace;
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }

        private void parseAction(String action) {
            int offset = action.lastIndexOf('_');
            if (0 < offset && ++offset < action.length()) {
                /*
                 * To be compatible with the old permission mechanism,
                 * here is only to provide pre-control by extract the
                 * resource_action {vertex/edge/schema}_{read/write},
                 * resource_action like vertex_read.
                 */
                String resource = action.substring(0, offset - 1);
                this.resource = ResourceType.valueOf(resource.toUpperCase());
                action = action.substring(offset);
            }
            this.action = HugePermission.valueOf(action.toUpperCase());
        }

        public static String roleFor(String graphSpace, String owner, HugePermission perm) {
            /*
             * Construct required permission such as
             *  $owner=graph1 $action=read
             *  (means required read permission of any one resource)
             *
             * In the future maybe also support:
             *  $owner=graph1 $action=vertex_read
             */
            return String.format("%s=%s %s=%s %s=%s", KEY_GRAPHSPACE, graphSpace,
                                 KEY_OWNER, owner, KEY_ACTION, perm.string());
        }

        public static RequiredPerm fromJson(String json) {
            return JsonUtil.fromJson(json, RequiredPerm.class);
        }

        public ResourceObject<?> resourceObject() {
            Nameable elem = HugeResource.NameObject.ANY;
            return ResourceObject.of(this.graphSpace, this.owner, this.resource, elem);
        }
    }
}
