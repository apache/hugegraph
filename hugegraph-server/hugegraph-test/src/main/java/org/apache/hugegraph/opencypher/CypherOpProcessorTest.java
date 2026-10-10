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

package org.apache.hugegraph.opencypher;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.testutil.Assert;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.junit.Test;
import org.opencypher.gremlin.traversal.ParameterNormalizer;
import org.mockito.Mockito;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;

public class CypherOpProcessorTest {

    private final CypherOpProcessor processor = new CypherOpProcessor();

    @Test
    public void testAcceptCypherParameterNamesAndNullValues() throws Exception {
        Map<String, Object> bindings = new HashMap<>();
        bindings.put("value", null);
        bindings.put("id", 1);
        bindings.put("label", "person");
        Assert.assertFalse(this.processor.validateEvalMessage(request("RETURN $value", bindings)).isPresent());
        Assert.assertFalse(this.processor.validateEvalMessage(RequestMessage.build(Tokens.OPS_EVAL)
                .addArg(Tokens.ARGS_GREMLIN, "RETURN 1").create()).isPresent());
    }

    @Test
    public void testRejectInvalidQuery() {
        assertInvalid(request(1, Collections.emptyMap()));
        assertInvalid(request(" \n", Collections.emptyMap()));
    }

    @Test
    public void testRejectInvalidBindingShapeAndKeys() {
        assertInvalid(request("RETURN 1", Collections.emptyList()));
        assertInvalid(request("RETURN 1", Collections.singletonMap(1, "value")));
        assertInvalid(request("RETURN 1", Collections.singletonMap(null, "value")));
    }

    @Test
    public void testRejectTranslatorNullSentinelInBindings() {
        Object reserved = ParameterNormalizer.normalize(Collections.singletonMap("value", null)).get("value");
        Assert.assertEquals("  cypher.null", reserved);
        assertInvalid(request("RETURN $value", Collections.singletonMap("value", reserved)));
        assertInvalid(request("RETURN $value", Collections.singletonMap("value",
                Collections.singleton(reserved))));
        assertInvalid(request("RETURN $value", Collections.singletonMap("value",
                Collections.singletonMap("nested", reserved))));
        assertInvalid(request("RETURN $value", Collections.singletonMap("value",
                Arrays.asList(null, Collections.singletonMap("nested", Collections.singletonList(reserved))))));
    }

    @Test
    public void testAcceptNullAndStringsNearTranslatorSentinel() throws Exception {
        Map<String, Object> bindings = Collections.singletonMap("value",
                Arrays.asList(null, "cypher.null", " cypher.null", "  cypher.null ",
                              Collections.singletonMap("nested", null)));
        Assert.assertFalse(this.processor.validateEvalMessage(request("RETURN $value", bindings)).isPresent());
    }

    @Test
    public void testEnforceParameterCountBoundary() throws Exception {
        Map<String, Object> bindings = new HashMap<>();
        for (int i = 0; i < AbstractEvalOpProcessor.DEFAULT_MAX_PARAMETERS; i++) {
            bindings.put("parameter" + i, i);
        }
        Assert.assertFalse(this.processor.validateEvalMessage(request("RETURN 1", bindings)).isPresent());
        bindings.put("oneTooMany", 1);
        assertInvalid(request("RETURN 1", bindings));
    }

    @Test
    public void testRollbackOnTraversalError() throws Exception {
        assertRollbackOnTraversalError(false);
    }

    @Test
    public void testRollbackFailureIsSuppressedOnTraversalError() throws Exception {
        assertRollbackOnTraversalError(true);
    }

    private void assertRollbackOnTraversalError(boolean failRollback) throws Exception {
        Context context = Mockito.mock(Context.class);
        GraphManager manager = Mockito.mock(GraphManager.class);
        GremlinExecutor gremlinExecutor = Mockito.mock(GremlinExecutor.class);
        AtomicReference<FutureTask<?>> submitted = new AtomicReference<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                                                             new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable command) {
                submitted.set((FutureTask<?>) command);
                super.execute(command);
            }
        };
        ChannelHandlerContext channelContext = Mockito.mock(ChannelHandlerContext.class);
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(StateKey.USE_BINARY).set(false);
        Settings settings = new Settings();
        settings.evaluationTimeout = 0;
        settings.strictTransactionManagement = false;
        Mockito.when(context.getRequestMessage()).thenReturn(request("RETURN 1", Collections.emptyMap()));
        Mockito.when(context.getSettings()).thenReturn(settings);
        Mockito.when(context.getGraphManager()).thenReturn(manager);
        Mockito.when(context.getGremlinExecutor()).thenReturn(gremlinExecutor);
        Mockito.when(gremlinExecutor.getExecutorService()).thenReturn(executor);
        Mockito.when(context.getChannelHandlerContext()).thenReturn(channelContext);
        Mockito.when(channelContext.channel()).thenReturn(channel);

        AssertionError failure = new AssertionError("fatal traversal failure");
        IllegalStateException rollbackFailure = new IllegalStateException("rollback failure");
        CountDownLatch responded = new CountDownLatch(1);
        AtomicReference<ResponseMessage> response = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            response.set(invocation.getArgument(0));
            responded.countDown();
            return null;
        }).when(context).writeAndFlush(Mockito.any(ResponseMessage.class));
        AtomicReference<Thread> iterationThread = new AtomicReference<>();
        AtomicReference<Thread> rollbackThread = new AtomicReference<>();
        Iterator<?> iterator = Mockito.mock(Iterator.class);
        Mockito.when(iterator.hasNext()).thenAnswer(invocation -> {
            iterationThread.set(Thread.currentThread());
            throw failure;
        });
        Mockito.doAnswer(invocation -> {
            rollbackThread.set(Thread.currentThread());
            if (failRollback) {
                throw rollbackFailure;
            }
            return null;
        }).when(manager).rollbackAll();
        GraphTraversal<?, ?> source = Mockito.mock(GraphTraversal.class);
        Method handleTraversal = CypherOpProcessor.class.getDeclaredMethod(
                "handleTraversal", Context.class, Iterator.class, GraphTraversal.class);
        handleTraversal.setAccessible(true);
        try {
            handleTraversal.invoke(this.processor, context, iterator, source);
            Assert.assertTrue("Fatal errors must send a terminal response",
                              responded.await(5, TimeUnit.SECONDS));
            FutureTask<?> task = submitted.get();
            ExecutionException result = Assert.assertThrows(ExecutionException.class,
                    () -> task.get(5, TimeUnit.SECONDS));
            Assert.assertSame(failure, result.getCause());
            Assert.assertNotNull(iterationThread.get());
            Assert.assertNotSame(Thread.currentThread(), iterationThread.get());
            Assert.assertSame(iterationThread.get(), rollbackThread.get());
            Mockito.verify(manager).rollbackAll();
            Mockito.verify(manager, Mockito.never()).commitAll();
            Mockito.verify(source).close();
            Mockito.verify(context).writeAndFlush(Mockito.any(ResponseMessage.class));
            Assert.assertEquals(ResponseStatusCode.SERVER_ERROR, response.get().getStatus().getCode());
            Assert.assertEquals(failure.getMessage(), response.get().getStatus().getMessage());
            Assert.assertNull(response.get().getResult().getData());
            Assert.assertEquals(failRollback ? 1 : 0, failure.getSuppressed().length);
            if (failRollback) {
                Assert.assertSame(rollbackFailure, failure.getSuppressed()[0]);
            }
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            channel.finishAndReleaseAll();
        }
    }

    private void assertInvalid(RequestMessage message) {
        OpProcessorException error = Assert.assertThrows(OpProcessorException.class,
                () -> this.processor.validateEvalMessage(message));
        Assert.assertEquals(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS,
                            error.getResponseMessage().getStatus().getCode());
    }

    private static RequestMessage request(Object query, Object bindings) {
        return RequestMessage.build(Tokens.OPS_EVAL)
                             .addArg(Tokens.ARGS_GREMLIN, query)
                             .addArg(Tokens.ARGS_BINDINGS, bindings).create();
    }
}
