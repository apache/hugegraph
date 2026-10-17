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

package org.apache.hugegraph.api.arthas;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.taobao.arthas.agent.attach.ArthasAgent;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;

@Path("arthas")
@Singleton
@Tag(name = "ArthasAPI")
public class ArthasAPI extends API {

    private static final Logger LOG = Log.logger(ArthasAPI.class);

    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;

    @PUT
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    @Operation(summary = "start arthas agent")
    public Object startArthas(@Context GraphManager manager,
                             @Context SecurityContext context) {
        checkAccess(manager, context);
        HugeConfig config = this.configProvider.get();
        HashMap<String, String> configMap = arthasConfig(config);
        String response = JsonUtil.toJson(configMap);
        // Keep the optional MCP endpoint disabled without extending the activation response.
        configMap.put("arthas.mcpEndpoint", "");
        // Do not let external Arthas properties override the configured listener.
        configMap.put("arthas.config.overrideAll", "false");
        configMap.put("arthas.tunnelServer", "");
        ArthasAgent.attach(configMap);
        return response;
    }

    static void checkAccess(GraphManager manager, SecurityContext context) {
        // No-auth requests receive an anonymous admin role, so a role check alone is insufficient.
        if (!manager.requireAuthentication()) {
            throw new ForbiddenException("HTTP Arthas activation requires authentication; " +
                                         "use bin/arthas.sh on the server host instead");
        }
        if (context == null || context.getUserPrincipal() == null ||
            HugeAuthenticator.USER_ANONY.equals(context.getUserPrincipal().getName()) ||
            !context.isUserInRole("admin")) {
            throw new ForbiddenException("Arthas activation requires an authenticated administrator");
        }
    }

    static HashMap<String, String> arthasConfig(HugeConfig config) {
        HashMap<String, String> configMap = new HashMap<>(4);
        configMap.put("arthas.telnetPort",
                      option(config, ServerOptions.ARTHAS_TELNET_PORT,
                             ServerOptions.ARTHAS_TELNET_PORT_LEGACY));
        configMap.put("arthas.httpPort",
                      option(config, ServerOptions.ARTHAS_HTTP_PORT,
                             ServerOptions.ARTHAS_HTTP_PORT_LEGACY));
        String ip = config.get(ServerOptions.ARTHAS_IP);
        ip = "localhost".equalsIgnoreCase(ip) ? "127.0.0.1" : ip;
        try {
            InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid or unresolvable Arthas bind address '" + ip + "'", e);
        }
        configMap.put("arthas.ip", ip);
        configMap.put("arthas.disabledCommands",
                      option(config, ServerOptions.ARTHAS_DISABLED_COMMANDS,
                             ServerOptions.ARTHAS_DISABLED_COMMANDS_LEGACY));
        return configMap;
    }

    private static String option(HugeConfig config, ConfigOption<String> canonical,
                                 ConfigOption<String> legacy) {
        if (!config.containsKey(canonical.name())) {
            return config.get(legacy);
        }
        String value = config.get(canonical);
        if (config.containsKey(legacy.name()) && !value.equals(config.get(legacy))) {
            LOG.warn("Conflicting Arthas options '{}' and '{}': '{}' takes precedence",
                     canonical.name(), legacy.name(), canonical.name());
        }
        return value;
    }
}
