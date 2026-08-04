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

import java.util.Map;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.GraphStatus;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.Test;

public class GraphStatusEntryTest {

    private static final String SERVER_KEY = "server";
    private static final String STATUS_KEY = "status";
    private static final String MESSAGE_KEY = "message";
    private static final String UPDATE_TIME_KEY = "update_time";

    @Test
    public void testJsonRoundTrip() {
        GraphStatusEntry entry = new GraphStatusEntry("server-1",
                                                      GraphStatus.FAILED,
                                                      "open failed", 1234L);

        GraphStatusEntry parsed =
                GraphStatusEntry.fromValue(JsonUtil.toJson(entry));

        assertEntry(parsed, "server-1", GraphStatus.FAILED, "open failed",
                    1234L);
    }

    @Test
    public void testJsonRoundTripWithoutMessage() {
        GraphStatusEntry entry = new GraphStatusEntry("server-1",
                                                      GraphStatus.READY,
                                                      null, 1234L);

        GraphStatusEntry parsed =
                GraphStatusEntry.fromValue(JsonUtil.toJson(entry));

        assertEntry(parsed, "server-1", GraphStatus.READY, null, 1234L);
    }

    @Test
    public void testFromValueReturnsNullForEmptyPayload() {
        Assert.assertNull(GraphStatusEntry.fromValue(null));
        Assert.assertNull(GraphStatusEntry.fromValue(""));
        Assert.assertNull(GraphStatusEntry.fromValue("   "));
    }

    @Test
    public void testFromValueReturnsNullForMalformedJson() {
        // A malformed payload written by another server must not break the
        // reader, it is dropped instead
        Assert.assertNull(GraphStatusEntry.fromValue("{not-json"));
        Assert.assertNull(GraphStatusEntry.fromValue("not json at all"));
        Assert.assertNull(GraphStatusEntry.fromValue("[]"));
    }

    @Test
    public void testFromValueIgnoresUnknownField() {
        // A value written by a newer server carries fields this server does
        // not know, the known fields must still be read
        String value = "{\"server\":\"server-1\",\"status\":\"READY\"," +
                       "\"update_time\":7,\"unknown_field\":\"x\"}";

        GraphStatusEntry parsed = GraphStatusEntry.fromValue(value);

        assertEntry(parsed, "server-1", GraphStatus.READY, null, 7L);
    }

    @Test
    public void testFromValueReturnsNullForUnknownStatus() {
        String value = "{\"server\":\"server-1\",\"status\":\"BOGUS\"}";

        Assert.assertNull(GraphStatusEntry.fromValue(value));
    }

    @Test
    public void testAsMapOmitsNullMessage() {
        GraphStatusEntry entry = new GraphStatusEntry("server-1",
                                                      GraphStatus.READY,
                                                      null, 42L);

        Map<String, Object> map = entry.asMap();

        Assert.assertFalse(map.containsKey(MESSAGE_KEY));
        Assert.assertEquals("server-1", map.get(SERVER_KEY));
        Assert.assertEquals(GraphStatus.READY.name(), map.get(STATUS_KEY));
        Assert.assertEquals(42L, map.get(UPDATE_TIME_KEY));
    }

    @Test
    public void testAsMapIncludesMessage() {
        GraphStatusEntry entry = new GraphStatusEntry("server-1",
                                                      GraphStatus.FAILED,
                                                      "open failed", 42L);

        Map<String, Object> map = entry.asMap();

        Assert.assertEquals("open failed", map.get(MESSAGE_KEY));
        Assert.assertEquals(GraphStatus.FAILED.name(), map.get(STATUS_KEY));
    }

    @Test
    public void testAsMapOfEmptyEntry() {
        Map<String, Object> map = new GraphStatusEntry().asMap();

        Assert.assertTrue(map.containsKey(STATUS_KEY));
        Assert.assertNull(map.get(SERVER_KEY));
        Assert.assertNull(map.get(STATUS_KEY));
        Assert.assertFalse(map.containsKey(MESSAGE_KEY));
        Assert.assertEquals(0L, map.get(UPDATE_TIME_KEY));
    }

    private static void assertEntry(GraphStatusEntry entry, String server,
                                    GraphStatus status, String message,
                                    long updateTime) {
        Assert.assertNotNull(entry);
        Assert.assertEquals(server, entry.server());
        Assert.assertEquals(status, entry.status());
        Assert.assertEquals(message, entry.message());
        Assert.assertEquals(updateTime, entry.updateTime());
    }
}
