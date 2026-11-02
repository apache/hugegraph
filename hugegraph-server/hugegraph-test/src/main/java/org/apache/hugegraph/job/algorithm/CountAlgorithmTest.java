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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class CountAlgorithmTest {

    private final boolean vertices;
    private final Algorithm algorithm;
    private UserJob<Object> job;
    private TestCursor cursor;

    public CountAlgorithmTest(String elements) {
        this.vertices = "vertices".equals(elements);
        this.algorithm = this.vertices ? new CountVertexAlgorithm() : new CountEdgeAlgorithm();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> elements() {
        return List.of(new Object[]{"vertices"}, new Object[]{"edges"});
    }

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.option(CoreOptions.OLTP_COLLECTION_TYPE)).thenReturn(CollectionType.JCF);
        this.job = Mockito.mock(UserJob.class);
        Mockito.when(this.job.graph()).thenReturn(graph);
        this.cursor = new TestCursor();
        if (this.vertices) {
            Mockito.doReturn(this.cursor).when(graph).vertices(Mockito.any(Query.class));
        } else {
            Mockito.doReturn(this.cursor).when(graph).edges(Mockito.any(Query.class));
        }
    }

    @Test
    public void testCountsLabelsAndTotalAndClosesScan() {
        this.addElement("person");
        this.addElement("software");
        this.addElement("person");
        Object result = this.algorithm.call(this.job, Map.of());
        Assert.assertEquals(Map.of("person", 2, "software", 1, "*", 3),
                            JsonUtil.fromJson(JsonUtil.toJson(result), Map.class));
        Assert.assertEquals(1, this.cursor.closes);
        Mockito.verify(this.job).updateProgress(1);
        Mockito.verify(this.job).updateProgress(2);
        Mockito.verify(this.job).updateProgress(3);
    }

    @Test
    public void testEmptyScanReturnsZeroAndCloses() {
        Object result = this.algorithm.call(this.job, Map.of());
        Assert.assertEquals(Map.of("*", 0), JsonUtil.fromJson(JsonUtil.toJson(result), Map.class));
        Assert.assertEquals(1, this.cursor.closes);
        Mockito.verify(this.job, Mockito.never()).updateProgress(Mockito.anyInt());
    }

    @Test
    public void testReadFailureRemainsPrimaryWhenClosingFails() {
        this.addElement("person");
        RuntimeException read = new IllegalStateException("scan failed");
        RuntimeException close = new IllegalStateException("close failed");
        this.cursor.readFailure = read;
        this.cursor.failAfter = 1;
        this.cursor.closeFailure = close;
        Throwable thrown = Assert.assertThrows(IllegalStateException.class,
                () -> this.algorithm.call(this.job, Map.of()));
        this.assertFailureAndClosed(read, close, thrown);
        Mockito.verify(this.job).updateProgress(1);
    }

    @Test
    public void testProgressFailureRemainsPrimaryWhenClosingFails() {
        this.addElement("person");
        RuntimeException progress = new IllegalStateException("progress save failed");
        RuntimeException close = new IllegalStateException("close failed");
        Mockito.doThrow(progress).when(this.job).updateProgress(1);
        this.cursor.closeFailure = close;
        Throwable thrown = Assert.assertThrows(IllegalStateException.class,
                () -> this.algorithm.call(this.job, Map.of()));
        this.assertFailureAndClosed(progress, close, thrown);
    }

    @Test
    public void testCloseFailureAfterCountingIsReported() {
        this.addElement("person");
        RuntimeException close = new IllegalStateException("close failed");
        this.cursor.closeFailure = close;
        Throwable thrown = Assert.assertThrows(RuntimeException.class,
                () -> this.algorithm.call(this.job, Map.of()));
        Assert.assertSame(close, HugeException.rootCause(thrown));
        Assert.assertEquals(1, this.cursor.closes);
    }

    @Test
    public void testFatalReadErrorRemainsPrimaryWhenClosingFails() {
        Error read = new AssertionError("fatal scan failure");
        RuntimeException close = new IllegalStateException("close failed");
        this.cursor.readFailure = read;
        this.cursor.closeFailure = close;
        Throwable thrown = Assert.assertThrows(AssertionError.class,
                () -> this.algorithm.call(this.job, Map.of()));
        this.assertFailureAndClosed(read, close, thrown);
    }

    private void addElement(String label) {
        Element element = this.vertices ? Mockito.mock(Vertex.class) : Mockito.mock(Edge.class);
        Mockito.when(element.label()).thenReturn(label);
        this.cursor.elements.add(element);
    }

    private void assertFailureAndClosed(Throwable primary, Throwable close, Throwable thrown) {
        Assert.assertSame(primary, thrown);
        Assert.assertEquals(1, thrown.getSuppressed().length);
        Assert.assertSame(close, HugeException.rootCause(thrown.getSuppressed()[0]));
        Assert.assertEquals(1, this.cursor.closes);
    }

    private static final class TestCursor implements CloseableIterator<Element> {

        private final List<Element> elements = new ArrayList<>();
        private Throwable readFailure;
        private RuntimeException closeFailure;
        private int position;
        private int failAfter;
        private int closes;

        @Override
        public boolean hasNext() {
            if (this.position == this.failAfter && this.readFailure != null) {
                if (this.readFailure instanceof Error error) {
                    throw error;
                }
                throw (RuntimeException) this.readFailure;
            }
            return this.position < this.elements.size();
        }

        @Override
        public Element next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return this.elements.get(this.position++);
        }

        @Override
        public void close() {
            this.closes++;
            if (this.closeFailure != null) {
                throw this.closeFailure;
            }
        }
    }
}
