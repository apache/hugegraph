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

package org.apache.hugegraph.cmd;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeGraph;
import org.junit.Assert;
import org.junit.Test;

public class InitStoreTest {

    @Test
    public void testInitBackendFailsFastForPermanentException() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        IllegalArgumentException exception =
                new IllegalArgumentException("Invalid backend configuration");
        HugeGraph graph = (HugeGraph) Proxy.newProxyInstance(
                HugeGraph.class.getClassLoader(), new Class<?>[]{HugeGraph.class},
                (proxy, invokedMethod, args) -> {
                    if (invokedMethod.getName().equals("initBackend")) {
                        invocations.incrementAndGet();
                        throw exception;
                    }
                    return null;
                });

        Method method = InitStore.class.getDeclaredMethod("initBackend",
                                                          HugeGraph.class);
        method.setAccessible(true);
        try {
            method.invoke(null, graph);
            Assert.fail("Expected initialization to fail");
        } catch (InvocationTargetException e) {
            Assert.assertSame(exception, e.getCause());
        }
        Assert.assertEquals(1, invocations.get());
    }
}
