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

package org.apache.hugegraph.store.client;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.HgStoreSession;
import org.junit.Test;

public class NodeTxExecutorTest {

    @Test
    public void testRetryReplacesSessionFromEvictedNode() {
        long nodeId = 1L;
        HgStoreNode oldNode = mock(HgStoreNode.class);
        HgStoreNode currentNode = mock(HgStoreNode.class);
        HgStoreNodeSession oldSession = mock(HgStoreNodeSession.class);
        HgStoreNodeSession currentSession = mock(HgStoreNodeSession.class);

        when(oldNode.getNodeId()).thenReturn(nodeId);
        when(currentNode.getNodeId()).thenReturn(nodeId);
        when(oldNode.openSession("graph")).thenReturn(oldSession);
        when(currentNode.openSession("graph")).thenReturn(currentSession);
        when(oldSession.getStoreNode()).thenReturn(oldNode);
        when(currentSession.getStoreNode()).thenReturn(currentNode);

        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        executor.setTx(true);
        AtomicInteger attempts = new AtomicInteger();
        Optional<HgStoreSession> result = executor.retryingInvoke(() -> {
            HgStoreNode node = attempts.getAndIncrement() == 0 ? oldNode : currentNode;
            HgStoreSession session = executor.openNodeSession(node);
            if (node == oldNode) {
                throw new RuntimeException("simulated transport failure");
            }
            return session;
        });

        assertSame(currentSession, result.get());
        verify(oldSession).beginTx();
        verify(currentSession).beginTx();
    }

    @Test
    public void testParallelReplacementUsesOneCurrentSession() throws Exception {
        long nodeId = 2L;
        HgStoreNode oldNode = mock(HgStoreNode.class);
        HgStoreNode currentNode = mock(HgStoreNode.class);
        HgStoreNodeSession oldSession = mock(HgStoreNodeSession.class);
        HgStoreNodeSession firstCurrentSession = mock(HgStoreNodeSession.class);
        HgStoreNodeSession secondCurrentSession = mock(HgStoreNodeSession.class);

        when(oldNode.getNodeId()).thenReturn(nodeId);
        when(currentNode.getNodeId()).thenReturn(nodeId);
        when(oldNode.openSession("graph")).thenReturn(oldSession);
        when(oldSession.getStoreNode()).thenReturn(oldNode);
        when(firstCurrentSession.getStoreNode()).thenReturn(currentNode);
        when(secondCurrentSession.getStoreNode()).thenReturn(currentNode);

        CountDownLatch concurrentCreations = new CountDownLatch(2);
        AtomicInteger creations = new AtomicInteger();
        when(currentNode.openSession("graph")).thenAnswer(invocation -> {
            int creation = creations.getAndIncrement();
            concurrentCreations.countDown();
            concurrentCreations.await(1, TimeUnit.SECONDS);
            return creation == 0 ? firstCurrentSession : secondCurrentSession;
        });

        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        executor.openNodeSession(oldNode);

        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        Future<HgStoreSession> first = workers.submit(() -> {
            start.await();
            return executor.openNodeSession(currentNode);
        });
        Future<HgStoreSession> second = workers.submit(() -> {
            start.await();
            return executor.openNodeSession(currentNode);
        });
        try {
            start.countDown();
            assertSame(first.get(3, TimeUnit.SECONDS),
                       second.get(3, TimeUnit.SECONDS));
            verify(currentNode, times(1)).openSession("graph");
        } finally {
            workers.shutdownNow();
        }
    }
}
