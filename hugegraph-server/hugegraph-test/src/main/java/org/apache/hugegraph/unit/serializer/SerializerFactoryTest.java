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

package org.apache.hugegraph.unit.serializer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.serializer.AbstractSerializer;
import org.apache.hugegraph.backend.serializer.BinaryScatterSerializer;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.backend.serializer.SerializerFactory;
import org.apache.hugegraph.backend.serializer.TextSerializer;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.Test;

public class SerializerFactoryTest extends BaseUnitTest {

    @Test
    public void testSerializer() {
        HugeConfig config = FakeObjects.newConfig();
        AbstractSerializer serializer = SerializerFactory.serializer(config, "text");
        Assert.assertEquals(TextSerializer.class, serializer.getClass());

        serializer = SerializerFactory.serializer(config, "binary");
        Assert.assertEquals(BinarySerializer.class, serializer.getClass());

        serializer = SerializerFactory.serializer(config, "binaryscatter");
        Assert.assertEquals(BinaryScatterSerializer.class,
                            serializer.getClass());

        Assert.assertThrows(BackendException.class, () -> {
            SerializerFactory.serializer(config, "invalid");
        }, e -> {
            Assert.assertContains("Not exists serializer:", e.getMessage());
        });
    }

    @Test
    public void testRegister() {
        HugeConfig config = FakeObjects.newConfig();
        SerializerFactory.register("fake", FakeSerializer.class.getName());
        Assert.assertEquals(FakeSerializer.class,
                            SerializerFactory.serializer(config, "fake").getClass());

        // Identical registration is idempotent
        SerializerFactory.register("fake", FakeSerializer.class.getName());
        Assert.assertEquals(FakeSerializer.class,
                            SerializerFactory.serializer(config, "fake").getClass());

        Assert.assertThrows(BackendException.class, () -> {
            // conflict
            SerializerFactory.register("fake", TextSerializer.class.getName());
        }, e -> {
            Assert.assertContains("Exists serializer:", e.getMessage());
        });

        Assert.assertThrows(BackendException.class, () -> {
            // invalid class
            SerializerFactory.register("fake", "org.apache.hugegraph.Invalid");
        }, e -> {
            Assert.assertContains("Invalid class:", e.getMessage());
        });

        Assert.assertThrows(BackendException.class, () -> {
            // subclass
            SerializerFactory.register("fake", "org.apache.hugegraph.HugeGraph");
        }, e -> {
            Assert.assertContains("Class is not a subclass of class",
                                  e.getMessage());
        });
    }

    @Test
    public void testRegisterConcurrently() throws Exception {
        String name = "concurrent-serializer-" + System.nanoTime();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<Class<?>>> registrations = List.of(
                    executor.submit(() -> {
                        ready.countDown();
                        start.await();
                        SerializerFactory.register(
                                name, FakeSerializer.class.getName());
                        return FakeSerializer.class;
                    }),
                    executor.submit(() -> {
                        ready.countDown();
                        start.await();
                        SerializerFactory.register(
                                name, TextSerializer.class.getName());
                        return TextSerializer.class;
                    }));

            Assert.assertTrue(ready.await(5L, TimeUnit.SECONDS));
            start.countDown();

            int successes = 0;
            int conflicts = 0;
            Class<?> registered = null;
            for (Future<Class<?>> registration : registrations) {
                try {
                    registered = registration.get(5L, TimeUnit.SECONDS);
                    successes++;
                } catch (ExecutionException e) {
                    Assert.assertInstanceOf(BackendException.class,
                                            e.getCause());
                    Assert.assertContains("Exists serializer:",
                                          e.getCause().getMessage());
                    conflicts++;
                }
            }

            Assert.assertEquals(1, successes);
            Assert.assertEquals(1, conflicts);
            Assert.assertNotNull(registered);

            SerializerFactory.register(name, registered.getName());
            HugeConfig config = FakeObjects.newConfig();
            Assert.assertEquals(
                    registered,
                    SerializerFactory.serializer(config, name).getClass());
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    public static class FakeSerializer extends BinarySerializer {

        public FakeSerializer(HugeConfig config) {
            super(config);
        }

        public FakeSerializer() {
            super(true, true, false);
        }
    }
}
