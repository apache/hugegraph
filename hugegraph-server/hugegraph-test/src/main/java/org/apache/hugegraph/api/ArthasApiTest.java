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

package org.apache.hugegraph.api;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hugegraph.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class ArthasApiTest extends BaseApiTest {

    private static final String ARTHAS_START_PATH = "/arthas";
    private static final String ARTHAS_API_BASE_URL = "http://127.0.0.1:8561";
    private static final String ARTHAS_API_PATH = "/api";

    @Test
    public void testArthasStart() {
        Response r = client().put(ARTHAS_START_PATH, "", "", ImmutableMap.of());
        String result = assertResponseStatus(200, r);
        Assert.assertEquals(4, JsonUtil.fromJson(result, Map.class).size());
        assertJsonContains(result, "arthas.telnetPort");
        assertJsonContains(result, "arthas.httpPort");
        assertJsonContains(result, "arthas.ip");
        assertJsonContains(result, "arthas.disabledCommands");
        Assert.assertEquals("127.0.0.1", assertJsonContains(result, "arthas.ip"));
    }

    @Test
    public void testAnonymousCannotActivate() {
        RestClient anonymous = new RestClient(BASE_URL, false);
        try {
            assertResponseStatus(401, anonymous.put(ARTHAS_START_PATH, "", "", ImmutableMap.of()));
        } finally {
            anonymous.close();
        }
    }

    @Test
    public void testNonAdminCannotActivate() {
        String username = "arthas_user_" + UUID.randomUUID().toString().replace("-", "");
        String usersPath = "graphspaces/DEFAULT/auth/users";
        String userBody = JsonUtil.toJson(ImmutableMap.of("user_name", username,
                                                         "user_password", "Arthas1234"));
        String created = assertResponseStatus(201, client().post(usersPath, userBody));
        String userId = assertJsonContains(created, "id");
        RestClient user = new RestClient(BASE_URL, username, "Arthas1234");
        try {
            assertResponseStatus(403, user.put(ARTHAS_START_PATH, "", "", ImmutableMap.of()));
        } finally {
            user.close();
            assertResponseStatus(204, client().delete(usersPath, userId));
        }
    }

    @Test
    public void testArthasApi() {
        testArthasStart();
        RestClient arthasApiClient = new RestClient(ARTHAS_API_BASE_URL, false);
        try {
            // A one-time command must succeed through its automatic session cleanup.
            String execBody = JsonUtil.toJson(ImmutableMap.of("action", "exec", "command", "version"));
            Response execResponse = arthasApiClient.post(ARTHAS_API_PATH, execBody);
            assertVersionSucceeded(assertResponseStatus(200, execResponse));
            assertResponseStatus(404, arthasApiClient.get("/mcp"));

            String sessionBody = JsonUtil.toJson(ImmutableMap.of("action", "init_session"));
            Response sessionResponse = arthasApiClient.post(ARTHAS_API_PATH, sessionBody);
            String sessionResult = assertResponseStatus(200, sessionResponse);
            Assert.assertEquals("SUCCEEDED", assertJsonContains(sessionResult, "state"));
            String sessionId = assertJsonContains(sessionResult, "sessionId");
            assertJsonContains(sessionResult, "consumerId");
            try {
                String sessionExecBody = JsonUtil.toJson(ImmutableMap.of(
                        "action", "exec", "command", "version", "sessionId", sessionId));
                Response sessionExecResponse = arthasApiClient.post(ARTHAS_API_PATH, sessionExecBody);
                assertVersionSucceeded(assertResponseStatus(200, sessionExecResponse));
            } finally {
                String closeBody = JsonUtil.toJson(ImmutableMap.of("action", "close_session", "sessionId", sessionId));
                Response closeResponse = arthasApiClient.post(ARTHAS_API_PATH, closeBody);
                String closeResult = assertResponseStatus(200, closeResponse);
                Assert.assertEquals("SUCCEEDED", assertJsonContains(closeResult, "state"));
            }

            String infoBody = JsonUtil.toJson(ImmutableMap.of("action", "session_info", "sessionId", sessionId));
            Response infoResponse = arthasApiClient.post(ARTHAS_API_PATH, infoBody);
            String infoResult = assertResponseStatus(200, infoResponse);
            Assert.assertEquals("FAILED", assertJsonContains(infoResult, "state"));
            Assert.assertEquals("session not found: " + sessionId, assertJsonContains(infoResult, "message"));

            String joinBody = JsonUtil.toJson(ImmutableMap.of("action", "join_session", "sessionId", "xxx"));
            Response joinResponse = arthasApiClient.post(ARTHAS_API_PATH, joinBody);
            String joinResult = assertResponseStatus(200, joinResponse);
            Assert.assertEquals("FAILED", assertJsonContains(joinResult, "state"));
            Assert.assertEquals("session not found: xxx", assertJsonContains(joinResult, "message"));
        } finally {
            arthasApiClient.close();
        }
    }

    private static void assertVersionSucceeded(String result) {
        Assert.assertEquals("SUCCEEDED", assertJsonContains(result, "state"));
        Map<?, ?> body = assertJsonContains(result, "body");
        Assert.assertEquals("TERMINATED", body.get("jobStatus"));
        Assert.assertEquals(false, body.get("timeExpired"));
        List<Map<?, ?>> results = assertMapContains(body, "results");
        Map<?, ?> version = assertArrayContains(results, "type", "version");
        Assert.assertEquals("4.1.5", version.get("version"));
        Map<?, ?> status = assertArrayContains(results, "type", "status");
        Assert.assertEquals(0, status.get("statusCode"));
    }
}
