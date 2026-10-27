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

package org.apache.hugegraph.security;

import java.lang.reflect.Method;

import javax.script.Bindings;
import javax.script.ScriptException;

import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("removal")
public class HugeSecurityManagerGrpcTest {

    private static final String ALLOCATOR_PROPERTY =
            "io.grpc.netty.shaded.io.grpc.netty.useCustomAllocator";

    private static GremlinGroovyScriptEngine engine;
    private static Bindings bindings;

    @BeforeClass
    public static void init() throws Exception {
        Log.logger(HugeSecurityManager.class);
        Method method = Class.forName("io.grpc.netty.shaded.io.grpc.netty.Utils")
                             .getDeclaredMethod("getByteBufAllocator", boolean.class);
        method.setAccessible(true);
        engine = new GremlinGroovyScriptEngine();
        bindings = engine.createBindings();
        bindings.put("allocator", new AllocatorAccess(method));
        // Initialize gRPC outside the sandbox: the regression is a property
        // read on every allocator call, including after initialization.
        engine.eval("allocator.allocate(false)", bindings);
        engine.eval("allocator.allocate(true)", bindings);
    }

    @Test
    public void testGrpcAllocatorFromGremlinWorker() throws Exception {
        assertAllocatorAllowed("gremlin-server-exec-grpc-regression");
    }

    @Test
    public void testGrpcAllocatorFromTaskWorker() throws Exception {
        assertAllocatorAllowed("task-worker-grpc-regression");
    }

    @Test
    public void testScriptCanReadAllocatorProperty() throws Exception {
        String expected = System.getProperty(ALLOCATOR_PROPERTY, "true");
        String script = "System.getProperty('" + ALLOCATOR_PROPERTY + "', 'true')";
        Assert.assertEquals(expected, eval(script, "gremlin-server-exec-grpc-regression"));
        Assert.assertEquals(expected, eval(script, "task-worker-grpc-regression"));
    }

    @Test
    public void testScriptCannotReadSensitiveProperty() {
        assertDenied("System.getProperty('user.home')", "system property(user.home)");
    }

    @Test
    public void testScriptCannotReadOtherGrpcProperty() {
        for (String key : new String[]{"io.grpc.netty.shaded.io.grpc.netty.unrelated",
                                       ALLOCATOR_PROPERTY + ".unrelated"}) {
            assertDenied("System.getProperty('" + key + "')", "system property(" + key + ")");
        }
    }

    @Test
    public void testScriptCannotEnumerateProperties() {
        assertDenied("System.getProperties()", "system properties");
    }

    @Test
    public void testAllocatorAccessDoesNotAllowSubsequentSensitiveRead() {
        assertDenied("allocator.allocate(false); System.getProperty('user.home')",
                     "system property(user.home)");
    }

    private static void assertAllocatorAllowed(String worker) throws Exception {
        for (boolean heap : new boolean[]{false, true}) {
            Object result = eval("allocator.allocate(" + heap + ")", worker);
            Assert.assertTrue(String.valueOf(result).contains("ByteBufAllocator"));
        }
    }

    private static void assertDenied(String script, String property) {
        Exception error = Assert.assertThrows(Exception.class, () ->
                eval(script, "gremlin-server-exec-grpc-regression"));
        Throwable cause = error;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        Assert.assertTrue(cause.toString(), cause instanceof SecurityException);
        Assert.assertEquals("Not allowed to access " + property + " via Gremlin", cause.getMessage());
    }

    private static Object eval(String script, String worker) throws ScriptException {
        engine.compile(script);
        Thread current = Thread.currentThread();
        String previousName = current.getName();
        SecurityManager previousManager = System.getSecurityManager();
        try {
            current.setName(worker);
            System.setSecurityManager(new HugeSecurityManager());
            // Calling the engine is essential: CompiledScript.eval() omits the
            // engine frame that HugeSecurityManager uses to identify Gremlin.
            return engine.eval(script, bindings);
        } finally {
            System.setSecurityManager(previousManager);
            current.setName(previousName);
        }
    }

    public static class AllocatorAccess {

        private final Method method;

        public AllocatorAccess(Method method) {
            this.method = method;
        }

        public String allocate(boolean heap) throws Exception {
            return this.method.invoke(null, heap).getClass().getName();
        }
    }
}
