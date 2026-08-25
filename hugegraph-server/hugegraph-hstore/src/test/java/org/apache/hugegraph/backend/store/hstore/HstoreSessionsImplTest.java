/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.store.hstore;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgStoreSession;
import org.junit.Assert;
import org.junit.Test;

public class HstoreSessionsImplTest {

    @Test
    public void testProductionClassDoesNotReferenceTestAssert() throws IOException {
        String className = HstoreSessionsImpl.class.getSimpleName() + ".class";
        try (InputStream stream = HstoreSessionsImpl.class.getResourceAsStream(className)) {
            Assert.assertNotNull(stream);
            String classFile = new String(stream.readAllBytes(), StandardCharsets.ISO_8859_1);
            Assert.assertFalse(classFile.contains(
                             "org/apache/hugegraph/testutil/Assert"));
        }
    }

    @Test
    public void testClearAndRecreateGraphRepeatedly() throws Exception {
        String graphName = "hugegraph/hstore-clear-test";
        AtomicInteger graphRegistrations = new AtomicInteger();
        AtomicInteger storeGraphDeletes = new AtomicInteger();
        AtomicInteger pdGraphDeletes = new AtomicInteger();
        HgStoreSession storeSession = (HgStoreSession) Proxy.newProxyInstance(
                HgStoreSession.class.getClassLoader(),
                new Class<?>[]{HgStoreSession.class},
                (proxy, method, args) -> {
                    Assert.assertEquals("deleteGraph", method.getName());
                    Assert.assertArrayEquals(new Object[]{graphName}, args);
                    storeGraphDeletes.incrementAndGet();
                    return true;
                });
        HstoreSessionsImpl.GraphStoreClient client =
                new HstoreSessionsImpl.GraphStoreClient() {
                    @Override
                    HgStoreSession openSession(String name) {
                        Assert.assertEquals(graphName, name);
                        return storeSession;
                    }

                    @Override
                    void setGraph(Metapb.Graph graph) {
                        Assert.assertEquals(graphName, graph.getGraphName());
                        graphRegistrations.incrementAndGet();
                    }

                    @Override
                    void delGraph(String name) throws PDException {
                        Assert.assertEquals(graphName, name);
                        pdGraphDeletes.incrementAndGet();
                        throw new PDException(0, "exercise clear() finally block");
                    }
                };
        HugeConfig config = new HugeConfig(Collections.emptyMap());
        HstoreSessionsImpl sessions = new HstoreSessionsImpl(
                config, "hugegraph", "hstore-clear-test", client);

        try {
            sessions.open();
            Assert.assertEquals(1, graphRegistrations.get());

            for (int i = 0; i < 50; i++) {
                sessions.clear();
                sessions.open();
                Assert.assertEquals(i + 2, graphRegistrations.get());
            }

            Assert.assertEquals(50, storeGraphDeletes.get());
            Assert.assertEquals(50, pdGraphDeletes.get());
        } finally {
            sessions.clear();
        }
    }
}
