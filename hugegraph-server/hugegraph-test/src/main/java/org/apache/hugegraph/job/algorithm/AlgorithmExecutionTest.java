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

package org.apache.hugegraph.job.algorithm;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.job.algorithm.AbstractAlgorithm.AlgoTraverser;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AlgorithmExecutionTest {

    private static final long CAPACITY = 1234L;

    private long previousCapacity;
    private AlgoTraverser traverser;
    private GraphTraversal<?, ?> traversal;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        this.previousCapacity = Query.defaultCapacity(CAPACITY);
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.option(CoreOptions.OLTP_COLLECTION_TYPE)).thenReturn(CollectionType.JCF);
        UserJob<Object> job = Mockito.mock(UserJob.class);
        Mockito.when(job.graph()).thenReturn(graph);
        this.traverser = new AlgoTraverser(job);
        this.traversal = Mockito.mock(GraphTraversal.class);
        Mockito.doAnswer(invocation -> {
            Assert.assertEquals(CAPACITY, Query.defaultCapacity());
            return null;
        }).when(this.traversal).close();
    }

    @After
    public void tearDown() {
        try {
            Assert.assertEquals(CAPACITY, Query.defaultCapacity());
        } finally {
            Query.defaultCapacity(this.previousCapacity);
        }
    }

    @Test
    public void testSuccessfulExecutionReturnsResultAndClosesTraversal() throws Exception {
        Object expected = new Object();
        Assert.assertSame(expected, this.traverser.execute(this.traversal, () -> {
            Assert.assertEquals(AbstractAlgorithm.MAX_QUERY_LIMIT, Query.defaultCapacity());
            return expected;
        }));
        Mockito.verify(this.traversal).close();
    }

    @Test
    public void testCheckedComputationFailureRetainsCloseFailure() throws Exception {
        Exception computation = new Exception("query failed");
        Exception close = new Exception("close failed");
        Mockito.doThrow(close).when(this.traversal).close();
        HugeException thrown = Assert.assertThrows(HugeException.class,
                () -> this.traverser.execute(this.traversal, () -> { throw computation; }));
        Assert.assertSame(computation, thrown.getCause());
        Assert.assertEquals("Failed to execute algorithm: query failed", thrown.getMessage());
        Assert.assertArrayEquals(new Throwable[]{close}, thrown.getSuppressed());
        Mockito.verify(this.traversal).close();
    }

    @Test
    public void testRuntimeComputationFailureWithoutMessageRetainsCloseError() throws Exception {
        RuntimeException computation = new IllegalStateException();
        Error close = new AssertionError("close failed");
        Mockito.doThrow(close).when(this.traversal).close();
        HugeException thrown = Assert.assertThrows(HugeException.class,
                () -> this.traverser.execute(this.traversal, () -> { throw computation; }));
        Assert.assertSame(computation, thrown.getCause());
        Assert.assertArrayEquals(new Throwable[]{close}, thrown.getSuppressed());
    }

    @Test
    public void testFatalComputationErrorRetainsCloseFailure() throws Exception {
        Error computation = new AssertionError("computation failed");
        Exception close = new Exception("close failed");
        Mockito.doThrow(close).when(this.traversal).close();
        Error thrown = Assert.assertThrows(AssertionError.class,
                () -> this.traverser.execute(this.traversal, () -> { throw computation; }));
        Assert.assertSame(computation, thrown);
        Assert.assertArrayEquals(new Throwable[]{close}, thrown.getSuppressed());
    }

    @Test
    public void testCloseFailureAfterSuccessKeepsExistingWrapper() throws Exception {
        Exception close = new Exception("close failed");
        Mockito.doThrow(close).when(this.traversal).close();
        HugeException thrown = Assert.assertThrows(HugeException.class,
                () -> this.traverser.execute(this.traversal, () -> 1));
        Assert.assertSame(close, thrown.getCause());
        Assert.assertEquals("Can't close traversal", thrown.getMessage());
    }

    @Test
    public void testCloseErrorAfterSuccessIsPropagatedUnchanged() throws Exception {
        Error close = new AssertionError("close failed");
        Mockito.doThrow(close).when(this.traversal).close();
        Assert.assertSame(close, Assert.assertThrows(AssertionError.class,
                () -> this.traverser.execute(this.traversal, () -> 1)));
    }
}
