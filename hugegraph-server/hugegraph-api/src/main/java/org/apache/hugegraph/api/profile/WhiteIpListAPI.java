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

package org.apache.hugegraph.api.profile;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("whiteiplist")
@Singleton
@Tag(name = "WhiteIpListAPI")
public class WhiteIpListAPI extends API {

    private static final Logger LOG = Log.logger(WhiteIpListAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    @Operation(summary = "list white ips")
    public Map<String, Object> list(@Context GraphManager manager) {
        LOG.debug("List white ips");
        AuthManager authManager = manager.authManager();
        Set<String> whiteIpList = authManager.listWhiteIPs();
        return ImmutableMap.of("whiteIpList", whiteIpList);
    }

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    @Operation(summary = "update white ip list")
    public Map<String, Object> updateWhiteIPs(@Context GraphManager manager,
                                              Map<String, Object> actionMap) {
        E.checkArgument(actionMap != null,
                        "Missing argument: actionMap");
        Set<String> whiteIpList = manager.authManager().listWhiteIPs();
        Object ipListRaw = actionMap.get("ips");
        E.checkArgument(ipListRaw instanceof List,
                        "Invalid ips type '%s', must be list", ipListRaw.getClass());
        List<String> ipList = (List<String>) ipListRaw;
        Object actionRaw = actionMap.get("action");
        E.checkArgument(actionRaw != null,
                        "Missing argument: action");
        E.checkArgument(actionRaw instanceof String,
                        "Invalid action type '%s', must be string",
                        actionRaw.getClass());
        String action = (String) actionRaw;
        E.checkArgument(StringUtils.isNotEmpty(action),
                        "Missing argument: action");
        Set<String> existedIPs = new HashSet<>();
        Set<String> loadedIPs = new HashSet<>();
        Set<String> illegalIPs = new HashSet<>();
        Map<String, Object> result = new HashMap<>();
        for (String ip : ipList) {
            if (whiteIpList.contains(ip)) {
                existedIPs.add(ip);
                continue;
            }
            if ("load".equals(action)) {
                boolean rightIp = checkIp(ip) ? loadedIPs.add(ip) : illegalIPs.add(ip);
            }
        }
        switch (action) {
            case "load":
                LOG.debug("Load to white ip list");
                result.put("existed_ips", existedIPs);
                result.put("added_ips", loadedIPs);
                if (!illegalIPs.isEmpty()) {
                    result.put("illegal_ips", illegalIPs);
                }
                whiteIpList.addAll(loadedIPs);
                break;
            case "remove":
                LOG.debug("Remove from white ip list");
                result.put("removed_ips", existedIPs);
                result.put("non_existed_ips", loadedIPs);
                whiteIpList.removeAll(existedIPs);
                break;
            default:
                throw new AssertionError(String.format("Invalid action '%s', " +
                                                       "supported action is " +
                                                       "'load' or 'remove'",
                                                       action));
        }
        manager.authManager().setWhiteIPs(whiteIpList);
        return result;
    }

    @PUT
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    @Operation(summary = "enable/disable the white ip list")
    public Map<String, Object> updateStatus(@Context GraphManager manager,
                                            @Parameter(description = "Status to set: " +
                                                                      "'true' to enable, " +
                                                                      "'false' to disable")
                                            @QueryParam("status") String status) {
        LOG.debug("Enable or disable white ip list");
        E.checkArgument("true".equals(status) ||
                        "false".equals(status),
                        "Invalid status, valid status is 'true' or 'false'");
        boolean open = Boolean.parseBoolean(status);
        manager.authManager().enabledWhiteIpList(open);
        Map<String, Object> map = new HashMap<>();
        map.put("WhiteIpListOpen", open);
        return map;
    }

    private boolean checkIp(String ipStr) {
        String ip = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
                    + "(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
        Pattern pattern = Pattern.compile(ip);
        Matcher matcher = pattern.matcher(ipStr);
        return matcher.matches();
    }
}
