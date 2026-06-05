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

package org.apache.hugegraph.MultiClusterTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.SimpleClusterTest.BaseSimpleTest;
import org.apache.hugegraph.SimpleClusterTest.BaseSimpleTest.RestClient;
import org.apache.hugegraph.ct.env.BaseEnv;
import org.apache.hugegraph.ct.env.MultiNodeEnv;
import org.apache.hugegraph.serializer.direct.util.HugeException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import jakarta.ws.rs.core.Response;

/**
 * MultiNode Test generate the cluster env with 3 pd node + 3 store node + 3 server node.
 * Or you can set different num of nodes by using env = new MultiNodeEnv(pdNum, storeNum, serverNum)
 * All nodes are deployed in ports generated randomly, the application of nodes are stored
 * in /apache-hugegraph-ct-1.7.0, you can visit each node with rest api.
 */
public class BaseMultiClusterTest {

    protected static BaseEnv env;
    protected static Process p;
    protected static List<RestClient> clients = new ArrayList<>();
    protected static String BASE_URL = "http://";
    protected static final String GRAPH = "hugegraphapi";
    protected static final String URL_PREFIX = "graphspaces/DEFAULT/graphs/" + GRAPH;
    protected static final String SCHEMA_PKS = "/schema/propertykeys";

    @BeforeClass
    public static void initEnv() {
        env = new MultiNodeEnv();
        env.startCluster();
        clients.clear();
        for (String addr : env.getServerRestAddrs()) {
            clients.add(new RestClient(BASE_URL + addr));
        }
        initGraph();
    }

    @AfterClass
    public static void clearEnv() {
        env.stopCluster();
        for (RestClient client : clients) {
            client.close();
        }
    }

    protected static void initGraph() {
        BaseSimpleTest.RestClient client = clients.get(0);
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
                throw new HugeException(String.format(
                        "Failed to initialize graph %s %s",GRAPH, r.readEntity(String.class)
                ));
            }
        }
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

    protected static String assertResponseStatus(int status,
                                                 Response response) {
        String content = response.readEntity(String.class);
        String message = String.format("Response with status %s and content %s",
                                       response.getStatus(), content);
        Assert.assertEquals(message, status, response.getStatus());
        return content;
    }

    public static Response createAndAssert(RestClient client, String path,
                                           String body,
                                           int status) {
        Response r = client.post(path, body);
        assertResponseStatus(status, r);
        return r;
    }
}
