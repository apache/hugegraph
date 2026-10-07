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

package org.apache.hugegraph.struct.schema;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.util.DateUtil;
import org.junit.Assert;
import org.junit.Test;

public class SchemaElementTest {

    private static final String CREATE_TIME = "2026-05-14 10:11:12.345";

    @Test
    public void testCreateTimeNormalizationAtSharedBoundary() {
        PropertyKey key = new PropertyKey(null, IdGenerator.of(1), "created");
        key.userdata(Userdata.CREATE_TIME, CREATE_TIME);
        Assert.assertEquals(DateUtil.parse(CREATE_TIME),
                            key.userdata().get(Userdata.CREATE_TIME));

        Date date = DateUtil.now();
        key.userdata(Userdata.CREATE_TIME, date);
        Assert.assertSame(date, key.userdata().get(Userdata.CREATE_TIME));
        key.userdata(Userdata.CREATE_TIME, "");
        Assert.assertEquals("", key.userdata().get(Userdata.CREATE_TIME));
        key.userdata("note", CREATE_TIME);
        Assert.assertEquals(CREATE_TIME, key.userdata().get("note"));
    }

    @Test
    public void testPersistedSchemaUserdataRestoresDate() {
        Map<String, Object> userdata = new HashMap<>();
        userdata.put(Userdata.CREATE_TIME, CREATE_TIME);
        userdata.put("count", 42);
        Map<String, Object> serialized = new HashMap<>();
        serialized.put(VertexLabel.P.ID, 1);
        serialized.put(VertexLabel.P.NAME, "person");
        serialized.put(VertexLabel.P.STATUS, SchemaStatus.CREATED.string());
        serialized.put(VertexLabel.P.USERDATA, userdata);

        VertexLabel label = VertexLabel.fromMap(serialized, null);
        Assert.assertEquals(DateUtil.parse(CREATE_TIME),
                            label.userdata().get(Userdata.CREATE_TIME));
        Assert.assertEquals(42, label.userdata().get("count"));
        Assert.assertEquals(SchemaStatus.CREATED, label.status());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCreateTimeRejected() {
        Userdata userdata = new Userdata();
        userdata.put(Userdata.CREATE_TIME, "not-a-date");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullBulkUserdataRejected() {
        new PropertyKey(null, IdGenerator.of(1), "created").userdata((Userdata) null);
    }

    @Test
    public void testCopyPreservesIdentityAndStatus() {
        PropertyKey key = new PropertyKey(null, IdGenerator.of(1), "created");
        key.userdata("note", "unchanged");
        key.status(SchemaStatus.CREATING);
        SchemaElement copy = key.copy();
        Assert.assertNotSame(key, copy);
        Assert.assertEquals(key, copy);
        Assert.assertEquals(key.hashCode(), copy.hashCode());
        Assert.assertEquals(key.status(), copy.status());
        Assert.assertEquals(key.userdata(), copy.userdata());
    }
}
