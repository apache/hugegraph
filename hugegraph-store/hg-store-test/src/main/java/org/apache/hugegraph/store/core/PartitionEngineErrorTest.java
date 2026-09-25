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

package org.apache.hugegraph.store.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.options.PartitionEngineOptions;
import org.junit.Test;

import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.State;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.error.RaftException;

public class PartitionEngineErrorTest {

    @Test
    public void testStateMachineErrorPreventsAutomaticRestart() {
        CountingEngine engine = new CountingEngine();
        engine.onError(new RaftException(ErrorType.ERROR_TYPE_STATE_MACHINE));
        engine.restartRaftNode();
        // A later error must not clear the terminal state either.
        engine.onError(new RaftException(ErrorType.ERROR_TYPE_LOG));
        assertEquals(0, engine.shutdowns);
        assertEquals(0, engine.starts);
    }

    @Test
    public void testOtherErrorsStillRestart() {
        CountingEngine engine = new CountingEngine();
        engine.onError(new RaftException(ErrorType.ERROR_TYPE_LOG));
        assertEquals(1, engine.shutdowns);
        assertEquals(1, engine.starts);
    }

    @Test
    public void testErrorWhileShutdownDrainsPreventsReinitialization() throws Exception {
        CountingEngine engine = new CountingEngine();
        CountDownLatch shuttingDown = new CountDownLatch(1);
        CountDownLatch drained = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        engine.duringShutdown = () -> {
            shuttingDown.countDown();
            try {
                assertTrue(drained.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        };
        try {
            Future<?> restart = executor.submit(engine::restartRaftNode);
            assertTrue(shuttingDown.await(5, TimeUnit.SECONDS));
            // An FSM error must be able to latch while restart waits for that FSM to drain.
            executor.submit(() -> engine.onError(
                    new RaftException(ErrorType.ERROR_TYPE_STATE_MACHINE))).get(5, TimeUnit.SECONDS);
            drained.countDown();
            restart.get(5, TimeUnit.SECONDS);
            assertEquals(1, engine.shutdowns);
            assertEquals(0, engine.starts);
        } finally {
            drained.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testQueuedActivityCheckCannotRestartAfterTerminalError() throws Exception {
        CountingEngine engine = new CountingEngine();
        NodeImpl node = mock(NodeImpl.class);
        Field nodeField = PartitionEngine.class.getDeclaredField("raftNode");
        nodeField.setAccessible(true);
        nodeField.set(engine, node);
        CountDownLatch checkingState = new CountDownLatch(1);
        CountDownLatch errorReported = new CountDownLatch(1);
        engine.restartFinished = new CountDownLatch(1);
        when(node.getNodeState()).thenAnswer(invocation -> {
            checkingState.countDown();
            assertTrue(errorReported.await(5, TimeUnit.SECONDS));
            return State.STATE_ERROR;
        });
        try {
            engine.checkActivity();
            assertTrue(checkingState.await(5, TimeUnit.SECONDS));
            engine.onError(new RaftException(ErrorType.ERROR_TYPE_STATE_MACHINE));
            errorReported.countDown();
            // Wait for the real activity check to reach and return from the restart gate.
            assertTrue(engine.restartFinished.await(5, TimeUnit.SECONDS));
            assertEquals(0, engine.shutdowns);
            assertEquals(0, engine.starts);
        } finally {
            errorReported.countDown();
        }
    }

    private static class CountingEngine extends PartitionEngine {

        private int shutdowns;
        private int starts;
        private Runnable duringShutdown;
        private CountDownLatch restartFinished;

        CountingEngine() {
            super(mock(HgStoreEngine.class), mock(ShardGroup.class));
        }

        @Override
        public Integer getGroupId() {
            return 1;
        }

        @Override
        public void restartRaftNode() {
            try {
                super.restartRaftNode();
            } finally {
                if (this.restartFinished != null) {
                    this.restartFinished.countDown();
                }
            }
        }

        @Override
        public void shutdown() {
            this.shutdowns++;
            if (this.duringShutdown != null) {
                this.duringShutdown.run();
            }
        }

        @Override
        public synchronized boolean init(PartitionEngineOptions options) {
            this.starts++;
            return true;
        }
    }
}
