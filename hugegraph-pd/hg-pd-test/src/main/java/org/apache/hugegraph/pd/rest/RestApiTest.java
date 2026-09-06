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
                                         .header("Authorization", "Basic c3RvcmU6MTIz")
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
                                         .header("Authorization", "Basic c3RvcmU6MTIz")
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }

    @Test
    public void testHealthNeedsNoAuth() throws URISyntaxException, IOException,
                                             InterruptedException {
        String url = pdRestAddr + "/v1/health";
        HttpRequest request = HttpRequest.newBuilder().uri(new URI(url)).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assert response.statusCode() == 200;
        // A 200 alone does not prove the path is anonymous: as of 1.7.0 the auth interceptor
        // refuses with 200 and an error envelope. checkHealthy() returns an empty body, which
        // separates the two whichever status a refusal carries.
        assert response.body().isEmpty() : "expected an empty body, got " + response.body();
    }

    @Test
    public void testReadyNeedsNoAuthAndReflectsRaft() throws URISyntaxException, IOException,
                                                            InterruptedException, JSONException {
        // The CI PD is a single-node raft group, so it is its own leader and must be ready
        String url = pdRestAddr + "/v1/ready";
        HttpRequest request = HttpRequest.newBuilder().uri(new URI(url)).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assert response.statusCode() == 200 : "expected 200, got " + response.statusCode() +
                                              " body=" + response.body();
        JSONObject obj = new JSONObject(response.body());
        assert obj.getBoolean("ready");
        assert obj.getBoolean("isLeader");
        assert "STATE_LEADER".equals(obj.getString("state"));
        // Unauthenticated, so it must not disclose cluster addresses
        assert !obj.has("leader") : "the anonymous body must not carry the leader address";
    }

    @Test
    public void testRaftGaugesExported() throws URISyntaxException, IOException,
                                                 InterruptedException {
        String url = pdRestAddr + "/actuator/prometheus";
        HttpRequest request = HttpRequest.newBuilder().uri(new URI(url)).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assert response.statusCode() == 200;
        String body = response.body();
        assert body.contains("hg_raft_leader{") : "missing hg_raft_leader gauge";
        assert body.contains("hg_raft_has_leader{") : "missing hg_raft_has_leader gauge";
        assert body.contains("hg_raft_alive_peers{") : "missing hg_raft_alive_peers gauge";
        // Single-node CI cluster: this PD is the leader and hears from itself
        assert body.matches("(?s).*hg_raft_leader\\{[^}]*\\} 1\\.0.*") :
                "hg_raft_leader should be 1 on a single-node leader";
        assert body.matches("(?s).*hg_raft_has_leader\\{[^}]*\\} 1\\.0.*") :
                "hg_raft_has_leader should be 1 on a single-node leader";
        assert body.matches("(?s).*hg_raft_alive_peers\\{[^}]*\\} 1\\.0.*") :
                "hg_raft_alive_peers should be 1 on a single-node leader";
    }

    @Test
    public void testQueryClusterMembers() throws URISyntaxException, IOException,
                                                 InterruptedException, JSONException {
        String url = pdRestAddr + "/v1/members";
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(new URI(url))
                                         .header("Authorization", "Basic c3RvcmU6MTIz")
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
                                         .header("Authorization", "Basic c3RvcmU6MTIz")
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
                                         .header("Authorization", "Basic c3RvcmU6MTIz")
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
                                         .header("Authorization", "Basic c3RvcmU6MTIz")
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
                                         .header("Authorization", "Basic c3RvcmU6MTIz")
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
                                         .header("Authorization", "Basic c3RvcmU6MTIz")
                                         .GET()
                                         .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject obj = new JSONObject(response.body());
        assert obj.getInt("status") == 0;
    }
}
