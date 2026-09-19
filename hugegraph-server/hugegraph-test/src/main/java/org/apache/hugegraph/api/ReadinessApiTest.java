/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.api;

import java.util.Map;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.Test;

import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.Response;

/**
 * The readiness endpoint answers 200 on a healthy server whatever the
 * backend: "embedded" for the in-process backends of the API suite, "hstore"
 * (with the storage fields) on the hstore job.
 */
public class ReadinessApiTest extends BaseApiTest {

    private static final String PATH = "/readiness";

    @Test
    public void testReadyOnAHealthyServer() {
        Response r = client().get(PATH);
        String result = assertResponseStatus(200, r);
        Map<String, Object> body = JsonUtil.fromJson(result, Map.class);
        Assert.assertEquals(true, body.get("ready"));
        Assert.assertTrue(String.valueOf(body.get("storage")),
                          "embedded".equals(body.get("storage")) ||
                          "hstore".equals(body.get("storage")));
        Assert.assertNotNull(body.get("reason"));
        if ("hstore".equals(body.get("storage"))) {
            Assert.assertEquals("ok", body.get("reason"));
            Assert.assertTrue(((Number) body.get("active_stores")).intValue() >= 1);
            Assert.assertNotNull(body.get("answered_store"));
            Assert.assertTrue(body.containsKey("cached"));
        }
    }

    /**
     * A Kubernetes httpGet probe carries no credential and no graphspace
     * prefix, so the endpoint must answer without either.
     */
    @Test
    public void testReadyWithoutCredentials() {
        Response r = ClientBuilder.newClient().target(BASE_URL + PATH)
                                  .request().get();
        try {
            Assert.assertEquals(200, r.getStatus());
            Assert.assertContains("\"ready\":true", r.readEntity(String.class));
        } finally {
            r.close();
        }
    }
}
