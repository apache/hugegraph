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

package org.apache.hugegraph.SimpleClusterTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hugegraph.ct.env.BaseEnv;
import org.apache.hugegraph.ct.env.SimpleEnv;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.serializer.direct.util.HugeException;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.google.common.collect.Multimap;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

/**
 * Simple Test generate the cluster env with 1 pd node + 1 store node + 1 server node.
 * All nodes are deployed in ports generated randomly; The application of nodes is stored
 * in /apache-hugegraph-ct-1.7.0, you can visit each node with rest api.
 */
public class BaseSimpleTest {

    protected static BaseEnv env;
    protected static Process p;
    protected static PDClient pdClient;

    protected static String BASE_URL = "http://";
    protected static final String GRAPH = "hugegraphapi";
    protected static final String USERNAME = "admin";
    private static final String PASSWORD = "pa";

    protected static final String URL_PREFIX = "graphspaces/DEFAULT/graphs/" + GRAPH;
    protected static final String SCHEMA_PKS = "/schema/propertykeys";
    protected static RestClient client;

    @BeforeClass
    public static void initEnv() {
        env = new SimpleEnv();
        env.startCluster();
        client = new RestClient(BASE_URL + env.getServerRestAddrs().get(0));
        initGraph();
    }

    @AfterClass
    public static void clearEnv() throws InterruptedException {
        env.stopCluster();
        Thread.sleep(2000);
        client.close();
    }

    protected String execCmd(String[] cmds) throws IOException {
        ProcessBuilder process = new ProcessBuilder(cmds);
        p = process.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            builder.append(line);
            builder.append(System.lineSeparator());
        }
        p.destroy();
        return builder.toString();
    }

    protected static void initGraph() {
        Response r = client.get(URL_PREFIX);
        if (r.getStatus() != 200) {
            String body = "{\n" +
                          "  \"backend\": \"hstore\",\n" +
                          "  \"serializer\": \"binary\",\n" +
                          "  \"store\": \"hugegraphapi\",\n" +
                          "  \"search.text_analyzer\": \"jieba\",\n" +
                          "  \"search.text_analyzer_mode\": \"INDEX\"\n" +
                          "}";
            r = client.post(URL_PREFIX, body);
            if (r.getStatus() != 201) {
                throw new HugeException("Failed to create graph: " + GRAPH +
                                        r.readEntity(String.class));
            }
        }
    }

    public static class RestClient {

        private final Client client;
        private final WebTarget target;

        public RestClient(String url) {
            this.client = ClientBuilder.newClient();
            this.client.register(EncodingFilter.class);
            this.client.register(GZipEncoder.class);
            this.client.register(HttpAuthenticationFeature.basic(USERNAME,
                                                                 PASSWORD));
            this.target = this.client.target(url);
        }

        public void close() {
            this.client.close();
        }

        public WebTarget target() {
            return this.target;
        }

        public WebTarget target(String url) {
            return this.client.target(url);
        }

        public Response get(String path) {
            return this.target.path(path).request().get();
        }

        public Response get(String path, String id) {
            return this.target.path(path).path(id).request().get();
        }

        public Response get(String path,
                            MultivaluedMap<String, Object> headers) {
            return this.target.path(path).request().headers(headers).get();
        }

        public Response get(String path, Multimap<String, Object> params) {
            WebTarget target = this.target.path(path);
            for (Map.Entry<String, Object> entries : params.entries()) {
                target = target.queryParam(entries.getKey(), entries.getValue());
            }
            return target.request().get();
        }

        public Response get(String path, Map<String, Object> params) {
            WebTarget target = this.target.path(path);
            for (Map.Entry<String, Object> i : params.entrySet()) {
                target = target.queryParam(i.getKey(), i.getValue());
            }
            return target.request().get();
        }

        public Response post(String path, String content) {
            return this.post(path, Entity.json(content));
        }

        public Response post(String path, Entity<?> entity) {
            return this.target.path(path).request().post(entity);
        }
    }

    protected static String assertResponseStatus(int status,
                                                 Response response) {
        String content = response.readEntity(String.class);
        String message = String.format("Response with status %s and content %s",
                                       response.getStatus(), content);
        Assert.assertEquals(message, status, response.getStatus());
        return content;
    }

    public static Response createAndAssert(String path, String body,
                                           int status) {
        Response r = client.post(path, body);
        assertResponseStatus(status, r);
        return r;
    }
}
