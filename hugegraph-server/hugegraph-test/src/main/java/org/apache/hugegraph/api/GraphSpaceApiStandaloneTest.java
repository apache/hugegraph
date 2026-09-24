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

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

/**
 * Tests that GraphSpaceAPI returns a friendly HTTP 400 error in standalone
 * mode (i.e. when the server is started without PD / hstore backend).
 * <p>
 * This class intentionally does NOT have a class-level Assume guard so that
 * the tests are actually executed in non-hstore CI runs.
 */
public class GraphSpaceApiStandaloneTest extends BaseApiTest {

    private static final String PATH = "graphspaces";

    @Before
    public void skipForPdMode() {
        assumeStandaloneMode();
    }

    @Test
    public void testProfileReturnsFriendlyError() {
        Response r = this.client().get(PATH + "/profile");
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testListReturnsFriendlyError() {
        Response r = this.client().get(PATH);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testGetReturnsFriendlyError() {
        Response r = this.client().get(PATH, "DEFAULT");
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testCreateReturnsFriendlyError() {
        String body = "{\"name\":\"test_standalone\",\"nickname\":\"test\","
                      + "\"description\":\"test\",\"cpu_limit\":10,"
                      + "\"memory_limit\":10,\"storage_limit\":10,"
                      + "\"max_graph_number\":10,\"max_role_number\":10,"
                      + "\"auth\":false,\"configs\":{}}";
        Response r = this.client().post(PATH, body);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testManageReturnsFriendlyError() {
        String body = "{\"action\":\"update\",\"update\":{\"name\":\"DEFAULT\"}}";
        Response r = this.client().put(PATH, "DEFAULT", body, ImmutableMap.of());
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testDeleteReturnsFriendlyError() {
        Response r = this.client().delete(PATH, "nonexistent");
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }
}
