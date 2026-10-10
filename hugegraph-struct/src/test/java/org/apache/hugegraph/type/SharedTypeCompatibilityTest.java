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

package org.apache.hugegraph.type;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.type.define.SerialEnum;
import org.junit.Assert;
import org.junit.Test;

public class SharedTypeCompatibilityTest {

    @Test
    public void testStoredTypeAndSchemaCodesRemainReadable() {
        Assert.assertEquals(HugeType.VERTEX, HugeType.fromCode((byte) 101));
        Assert.assertEquals(HugeType.EDGE_OUT, HugeType.fromCode((byte) 130));
        Assert.assertEquals(HugeType.EDGE_IN, HugeType.fromCode((byte) 140));
        Assert.assertEquals(HugeType.OLAP, HugeType.fromCode((byte) 106));
        Assert.assertEquals(HugeType.UNIQUE_INDEX, HugeType.fromCode((byte) 178));
        Assert.assertEquals(HugeType.SYS_SCHEMA, HugeType.fromCode((byte) 250));
        Assert.assertEquals(HugeType.MAX_TYPE, HugeType.fromCode((byte) 255));
        Assert.assertEquals((byte) 211, HugeKeys.SUB_LABEL.code());
        Assert.assertEquals((byte) 250, HugeKeys.AGGREGATE_PROPERTIES.code());
        Assert.assertEquals(EdgeLabelType.GENERAL,
                            SerialEnum.fromCode(EdgeLabelType.class, (byte) 4));
        Assert.assertEquals(SchemaStatus.CLEARING,
                            SerialEnum.fromCode(SchemaStatus.class, (byte) 7));
        Assert.assertEquals(Cardinality.SET,
                            SerialEnum.fromCode(Cardinality.class, (byte) 3));
        Assert.assertEquals(Directions.IN,
                            SerialEnum.fromCode(Directions.class, (byte) 2));
    }

    @Test
    public void testLazyAndConcurrentEnumRegistration() throws Exception {
        // This extension does not call register() in a static initializer.
        Assert.assertEquals(ExtensionType.VALUE,
                            SerialEnum.fromCode(ExtensionType.class, (byte) 42));
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int thread = 0; thread < 4; thread++) {
                tasks.add(() -> {
                    for (int iteration = 0; iteration < 1000; iteration++) {
                        SerialEnum.register(Cardinality.class);
                        SerialEnum.register(ExtensionType.class);
                        Assert.assertEquals(Cardinality.LIST,
                                            SerialEnum.fromCode(Cardinality.class, (byte) 2));
                        Assert.assertEquals(ExtensionType.VALUE,
                                            SerialEnum.fromCode(ExtensionType.class, (byte) 42));
                    }
                    return null;
                });
            }
            List<Future<Void>> results = executor.invokeAll(tasks, 10, TimeUnit.SECONDS);
            for (Future<Void> result : results) {
                Assert.assertFalse("Enum registration timed out", result.isCancelled());
                result.get();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    public enum ExtensionType implements SerialEnum {
        VALUE;

        @Override
        public byte code() {
            return 42;
        }
    }
}
