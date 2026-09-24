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

import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class UserApiTest extends BaseApiTest {

    private static final String PATH = "graphspaces/DEFAULT/auth/users";
    private static final int NO_LIMIT = -1;

    @Override
    @After
    public void teardown() throws Exception {
        super.teardown();
        Response r = this.client().get(PATH, ImmutableMap.of("limit", NO_LIMIT));
        String result = r.readEntity(String.class);
        Map<String, List<Map<String, Object>>> resultMap =
                JsonUtil.fromJson(result,
                                  new TypeReference<Map<String, List<Map<String, Object>>>>() {
                                  });
        List<Map<String, Object>> users = resultMap.get("users");
        for (Map<String, Object> user : users) {
            if ("admin".equals(user.get("user_name"))) {
                continue;
            }
            this.client().delete(PATH, (String) user.get("id"));
        }
    }

    @Test
    public void testCreate() {
        String user1 = "{\"user_name\":\"user1\",\"user_password\":\"p1\"," +
                       "\"user_email\":\"user1@baidu.com\",\"user_phone\":" +
                       "\"123456789\",\"user_avatar\":\"image1.jpg\"}";

        String user2 = "{\"user_name\":\"user2\",\"user_password\":\"p2\"," +
                       "\"user_email\":\"user2@baidu.com\"," +
                       "\"user_phone\":\"1357924680\"," +
                       "\"user_avatar\":\"image2.jpg\"}";

        Response r = client().post(PATH, user1);
        String result = assertResponseStatus(201, r);
        Response r2 = client().post(PATH, user2);
        String result2 = assertResponseStatus(201, r2);

        assertJsonContains(result, "user_name");
        assertJsonContains(result, "user_password");
        assertJsonContains(result, "user_email");
        assertJsonContains(result, "user_phone");
        assertJsonContains(result, "user_avatar");

        assertJsonContains(result2, "user_name");
        assertJsonContains(result2, "user_password");
        assertJsonContains(result2, "user_email");
        assertJsonContains(result2, "user_phone");
        assertJsonContains(result2, "user_avatar");

        Response r3 = client().post(PATH, "{}");
        assertResponseStatus(400, r3);

        String user3 = "{\"user_name\":\"user1\",\"user_password\":\"p1\"," +
                       "\"user_email\":\"user1@baidu.com\"," +
                       "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                       ".jpg\"}";
        Response r4 = client().post(PATH, user3);
        String result4 = assertResponseStatus(400, r4);
        String message = assertJsonContains(result4, "message");
        boolean containsExpected = message.contains("exist");
        Assert.assertTrue(containsExpected);
    }

    @Test
    public void testList() {
        createUser("test1");
        createUser("test2");
        createUser("test3");
        List<Map<String, Object>> users = listUsers();
        Assert.assertEquals(4, users.size());
    }

    @Test
    public void testGetUser() {
        createUser("test1");
        createUser("test2");
        List<Map<String, Object>> users = listUsers();
        for (Map<String, Object> user : users) {
            Response r = client().get(PATH, (String) user.get("id"));
            String result = assertResponseStatus(200, r);
            assertJsonContains(result, "user_name");
        }
    }

    @Test
    public void testUpdate() {
        createUser("test1");
        createUser("test2");
        List<Map<String, Object>> users = listUsers();
        for (Map<String, Object> user : users) {
            if ("admin".equals(user.get("user_name"))) {
                continue;
            }
            String user1 = "{\"user_password\":\"p1\"," +
                           "\"user_email\":\"user1@baidu.com\"," +
                           "\"user_phone\":\"111111\"," +
                           "\"user_avatar\":\"image1" +
                           ".jpg\"}";
            Response r = client().put(PATH, (String) user.get("id"), user1,
                                      ImmutableMap.of());
            assertResponseStatus(200, r);
        }
    }

    @Test
    public void testDelete() {
        createUser("test1");
        createUser("test2");
        createUser("test3");

        List<Map<String, Object>> users = listUsers();
        for (Map<String, Object> user : users) {
            if ("admin".equals(user.get("user_name"))) {
                continue;
            }
            Response r = client().delete(PATH, (String) user.get("id"));
            assertResponseStatus(204, r);
        }
        Response r = client().delete(PATH, "test1");
        String result = assertResponseStatus(400, r);
        String message = assertJsonContains(result, "message");
        boolean containsExpected = message.contains("Invalid user") ||
                                   message.contains("not exist");
        Assert.assertTrue(containsExpected);
    }

    protected void createUser(String name) {
        String user = "{\"user_name\":\"" + name + "\",\"user_password\":\"p1" +
                      "\", \"user_email\":\"user1@baidu.com\"," +
                      "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                      ".jpg\"}";
        Response r = this.client().post(PATH, user);
        assertResponseStatus(201, r);
    }

    protected List<Map<String, Object>> listUsers() {
        Response r = this.client().get(PATH, ImmutableMap.of("limit", NO_LIMIT));
        String result = assertResponseStatus(200, r);

        Map<String, List<Map<String, Object>>> resultMap =
                JsonUtil.fromJson(result,
                                  new TypeReference<Map<String, List<Map<String, Object>>>>() {
                                  });
        return resultMap.get("users");
    }
}
