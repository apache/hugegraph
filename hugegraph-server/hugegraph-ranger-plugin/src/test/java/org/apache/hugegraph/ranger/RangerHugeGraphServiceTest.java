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

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpServer;

public class RangerHugeGraphServiceTest {

    private static final String GRAPHS_PATH = "/graphspaces/DEFAULT/graphs";
    private static final String GOOD_AUTH = "Basic " + Base64.getEncoder().encodeToString(
            "admin:admin".getBytes(StandardCharsets.UTF_8));

    private HttpServer server;

    @Before
    public void startServer() throws Exception {
        // Mimics HugeGraph: /versions is public, the graphs list needs auth
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.server.createContext("/versions", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        this.server.createContext(GRAPHS_PATH, exchange -> {
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            if (!GOOD_AUTH.equals(auth)) {
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }
            byte[] body = "{\"graphs\":[\"hugegraph\"]}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        this.server.start();
    }

    @After
    public void stopServer() {
        this.server.stop(0);
    }

    @Test
    public void testValidateConfigAcceptsGoodCredentials() throws Exception {
        Map<String, Object> result = this.service("admin").validateConfig();
        Assert.assertEquals(true, result.get("connectivityStatus"));
    }

    @Test
    public void testValidateConfigDecryptsRangerEncryptedPassword() throws Exception {
        // Ranger Admin passes the password re-encrypted, never in plain text
        String encrypted = PasswordUtils.encryptPassword("admin");
        Assert.assertNotEquals("admin", encrypted);

        Map<String, Object> result = this.service(encrypted).validateConfig();
        Assert.assertEquals(true, result.get("connectivityStatus"));
    }

    @Test
    public void testValidateConfigRejectsBadCredentials() throws Exception {
        Map<String, Object> result = this.service("wrong").validateConfig();
        Assert.assertEquals(false, result.get("connectivityStatus"));
        Assert.assertTrue(String.valueOf(result.get("message")).contains("HTTP 401"));
    }

    private RangerHugeGraphService service(String password) {
        Map<String, String> configs = new HashMap<>();
        configs.put("hugegraph.url", "http://127.0.0.1:" + this.server.getAddress().getPort());
        configs.put("username", "admin");
        configs.put("password", password);

        RangerService rangerService = new RangerService();
        rangerService.setName("hugegraph");
        rangerService.setType("hugegraph");
        rangerService.setConfigs(configs);

        RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("hugegraph");

        RangerHugeGraphService service = new RangerHugeGraphService();
        service.init(serviceDef, rangerService);
        return service;
    }
}
