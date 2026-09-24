/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.meta;

import org.apache.hugegraph.meta.MetaManager.SchemaCacheClearEvent;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class MetaManagerSchemaCacheClearEventTest {

    @Test
    public void testFromValueReturnsNullForEmptyPayload() {
        Assert.assertNull(SchemaCacheClearEvent.fromValue(null));
        Assert.assertNull(SchemaCacheClearEvent.fromValue(""));
    }

    @Test
    public void testFromValueParsesLegacyPlainGraphName() {
        SchemaCacheClearEvent event =
                SchemaCacheClearEvent.fromValue("DEFAULT-graph1");

        assertEvent(event, "DEFAULT-graph1", null);
    }

    @Test
    public void testFromValueIgnoresMalformedJson() {
        Assert.assertNull(SchemaCacheClearEvent.fromValue("{not-json"));
    }

    @Test
    public void testFromValueParsesJsonWithSource() {
        String value = MetaManager.schemaCacheClearEventValue("g", "u");
        SchemaCacheClearEvent event = SchemaCacheClearEvent.fromValue(value);

        assertEvent(event, "g", "u");
    }

    @Test
    public void testFromValueParsesJsonWithoutSource() {
        SchemaCacheClearEvent event =
                SchemaCacheClearEvent.fromValue("{\"graph\":\"g\"}");

        assertEvent(event, "g", null);
    }

    @Test
    public void testFromValueIgnoresJsonWithoutGraph() {
        Assert.assertNull(
                SchemaCacheClearEvent.fromValue("{\"source\":\"u\"}"));
    }

    private static void assertEvent(SchemaCacheClearEvent event,
                                    String graph,
                                    String source) {
        Assert.assertNotNull(event);
        Assert.assertEquals(graph, event.graph());
        Assert.assertEquals(source, event.source());
    }
}
