/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.testutil;

import org.apache.hugegraph.api.BaseApiTest.RestClient;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class AuthApiUtils {

    private static final String PATH = "auth";
    // use authed as test space
    private static final String AUTH_PATH = "graphspaces/%s/auth";
    private static final String BELONG_PATH = AUTH_PATH + "/belongs";
    private static final String ROLE_PATH = AUTH_PATH + "/roles";
    private static final String ACCESS_PATH = AUTH_PATH + "/accesses";
    private static final String TARGET_PATH = AUTH_PATH + "/targets";
    private static final String SPACE_PATH = "graphspaces";
    private static final String USER_PATH = "auth/users";
    private static final String MANAGER_PATH = "auth/managers";
    private static final String SPACE_DEFAULT = "graphspaces/%s/role";

    public static Response createUser(RestClient client, String name,
                                      String password) {
        String user = "{\"user_name\":\"%s\",\"user_password\":\"%s" +
                      "\",\"user_email\":\"user1@baidu.com\"," +
                      "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                      ".jpg\"}";
        return client.post(USER_PATH, String.format(user, name, password));
    }

    public static Response createBelong(RestClient client,
                                        String graphSpace, String user,
                                        String role, String group) {
        String path = String.format(BELONG_PATH, graphSpace);
        String belong = "{\"user\":\"%s\",\"role\":\"%s\"," +
                        "\"group\": \"%s\"}";
        return client.post(path, String.format(belong, user, role, group));
    }

    public static Response createBelong(RestClient client, String graphSpace,
                                        String user, String role, String group,
                                        String link) {
        String path = String.format(BELONG_PATH, graphSpace);
        String belong = "{\"user\":\"%s\",\"role\":\"%s\"," +
                        "\"group\": \"%s\",\"link\": \"%s\"}";
        return client.post(path, String.format(belong, user, role, group,
                                               link));
    }

    public static Response createRole(RestClient client, String graphSpace,
                                      String name, String nickname) {
        String path = String.format(ROLE_PATH, graphSpace);
        String role = "{\"role_name\":\"%s\",\"role_nickname\":\"%s\"," +
                      "\"role_description\":\"api_test\"}";
        return client.post(path, String.format(role, name, nickname));
    }

    public static Response updateRole(RestClient client, String graphSpace,
                                      String name, String nickname) {
        String path = String.format(ROLE_PATH, graphSpace);
        String role = "{\"role_name\":\"%s\",\"role_nickname\":\"%s\"," +
                      "\"role_description\":\"api_test\"}";
        return client.put(path, name, String.format(role, name, nickname),
                          ImmutableMap.of());
    }

    public static Response createAccess(RestClient client, String graphSpace,
                                        String role, String target, String permission) {
        String path = String.format(ACCESS_PATH, graphSpace);
        String access = "{\"role\":\"%s\",\"target\":\"%s\"," +
                        "\"access_permission\": \"%s\"}";
        return client.post(path, String.format(access, role, target,
                                               permission));
    }

    public static Response createTarget(RestClient client, String graphSpace,
                                        String name, String graph) {
        String path = String.format(TARGET_PATH, graphSpace);
        String target = "{\"target_name\":\"%s\",\"target_graph\":\"%s\"," +
                        "\"target_description\": null," +
                        "\"target_resources\":[]}";
        return client.post(path, String.format(target, name, graph));
    }

    public static Response createManager(RestClient client, String user,
                                         String type, String space) {
        String body = "{\"user\":\"%s\",\"type\":\"%s\"," +
                      "\"graphspace\": \"%s\"}";
        return client.post(MANAGER_PATH, String.format(body, user, type,
                                                       space));
    }

    public static Response createDefaultRole(RestClient client,
                                             String graphSpace, String user,
                                             String role, String graph) {
        String path = String.format(SPACE_DEFAULT, graphSpace);
        String body = "{\"user\":\"%s\",\"role\":\"%s\"," +
                      "\"graph\": \"%s\"}";
        return client.post(path, String.format(body, user, role, graph));
    }
}
