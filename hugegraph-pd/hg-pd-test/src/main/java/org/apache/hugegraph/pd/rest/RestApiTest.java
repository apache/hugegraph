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

package org.apache.hugegraph.pd.rest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

public class RestApiTest extends BaseServerTest {

    @Test
    public void testQueryIndexInfo() throws URISyntaxException, IOException, InterruptedException,
                                            JSONException {
        String url = pdRestAddr + "/";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, VALID_AUTH)
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assert response.statusCode() == 200;
        JSONObject obj = new JSONObject(response.body());
        assert obj.getString("state") != null;
        assert obj.getString("leader") != null;
        assert obj.getInt("memberSize") > 0 : "memberSize should be > 0 for a running cluster";
        // storeSize can be 0 in PD-only test environments with no store nodes registered
        assert obj.getInt("storeSize") >= 0;
    }

    @Test
    public void testQueryClusterInfo() throws URISyntaxException, IOException, InterruptedException,
                                              JSONException {
        String url = pdRestAddr + "/v1/cluster";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, VALID_AUTH)
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testQueryClusterMembers() throws URISyntaxException, IOException,
                                                 InterruptedException, JSONException {
        String url = pdRestAddr + "/v1/members";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, VALID_AUTH)
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testQueryStoresInfo() throws URISyntaxException, IOException, InterruptedException,
                                             JSONException {
        String url = pdRestAddr + "/v1/stores";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, VALID_AUTH)
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testQueryGraphsInfo() throws IOException, InterruptedException, JSONException,
                                             URISyntaxException {
        String url = pdRestAddr + "/v1/graphs";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, VALID_AUTH)
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testQueryPartitionsInfo() throws IOException, InterruptedException, JSONException,
                                                 URISyntaxException {
        String url = pdRestAddr + "/v1/highLevelPartitions";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, VALID_AUTH)
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testQueryDebugPartitionsInfo() throws URISyntaxException, IOException,
                                                      InterruptedException {
        String url = pdRestAddr + "/v1/partitions";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, VALID_AUTH)
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assert response.statusCode() == 200;
    }

    @Test
    public void testQueryShards() throws URISyntaxException, IOException, InterruptedException,
                                         JSONException {
        String url = pdRestAddr + "/v1/shards";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, VALID_AUTH)
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testMissingCredentialGets401() throws URISyntaxException, IOException,
                                                      InterruptedException {
        String url = pdRestAddr + "/v1/members";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assert response.statusCode() == 401;
    }

    @Test
    public void testWrongPasswordGets401() throws URISyntaxException, IOException,
                                                  InterruptedException {
        String url = pdRestAddr + "/v1/members";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, basicAuth("store", "wrong-password"))
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assert response.statusCode() == 401;
    }

    @Test
    public void testEmptyPasswordGets401() throws URISyntaxException, IOException,
                                                  InterruptedException {
        String url = pdRestAddr + "/v1/members";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, basicAuth("hg", ""))
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assert response.statusCode() == 401;
    }

    private int statusWithoutCredential(String path) throws URISyntaxException, IOException,
                                                            InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(pdRestAddr + path))
                                         .GET()
                                         .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    @Test
    public void testProbePathsNeedNoCredential() throws URISyntaxException, IOException,
                                                        InterruptedException {
        assert statusWithoutCredential("/v1/health") == 200;
        assert statusWithoutCredential("/actuator/health") == 200;
        // Nested actuator paths are probe surface too. They stay open because
        // actuator has its own handler mapping that the auth interceptor is not
        // attached to, not because of the /actuator/** exclusion pattern.
        assert statusWithoutCredential("/actuator/metrics/jvm.memory.used") == 200;
    }

    @Test
    public void testUnexposedActuatorEndpointIsClosed() throws URISyntaxException, IOException,
                                                               InterruptedException {
        // not in management.endpoints.web.exposure.include, so it must never serve data
        assert statusWithoutCredential("/actuator/env") != 200;
    }

    @Test
    public void testUnknownServiceNameGets401() throws URISyntaxException, IOException,
                                                       InterruptedException {
        String url = pdRestAddr + "/v1/members";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header(AUTH_HEADER, basicAuth("nobody", SECRET))
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assert response.statusCode() == 401;
    }
}
