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

package org.apache.hugegraph.store.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.junit.Test;

import com.alipay.sofa.jraft.RaftGroupService;

public class PartitionEngineShutdownTest {

    @Test(timeout = 5000)
    public void testInterruptedJoinRetriesBeforeMarkingPartitionStopped() throws Exception {
        PartitionEngine engine = new PartitionEngine(mock(HgStoreEngine.class),
                                                     mock(ShardGroup.class)) {
            @Override
            public String toString() {
                return "test-partition";
            }
        };
        Field started = PartitionEngine.class.getDeclaredField("started");
        started.setAccessible(true);
        started.setBoolean(engine, true);
        RaftGroupService raft = mock(RaftGroupService.class);
        Field service = PartitionEngine.class.getDeclaredField("raftGroupService");
        service.setAccessible(true);
        service.set(engine, raft);
        AtomicInteger joins = new AtomicInteger();
        doAnswer(invocation -> {
            assertTrue("partition must remain started until Raft terminates",
                       started.getBoolean(engine));
            verify(raft).shutdown();
            if (joins.incrementAndGet() == 1) {
                assertTrue("model an interrupt restored by the shutdown listener",
                           Thread.interrupted());
                throw new InterruptedException("shutdown thread interrupted");
            }
            assertFalse("join retry must clear interruption while draining Raft",
                        Thread.currentThread().isInterrupted());
            return null;
        }).when(raft).join();
        try {
            Thread.currentThread().interrupt();
            engine.shutdown();
            assertEquals(2, joins.get());
            verify(raft, times(2)).join();
            assertFalse(started.getBoolean(engine));
            assertTrue("shutdown must restore caller interruption after Raft termination",
                       Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
