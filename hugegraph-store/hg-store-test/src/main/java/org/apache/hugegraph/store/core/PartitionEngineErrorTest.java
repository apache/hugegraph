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
import static org.mockito.Mockito.mock;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.options.PartitionEngineOptions;
import org.junit.Test;

import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.error.RaftException;

public class PartitionEngineErrorTest {

    @Test
    public void testStateMachineErrorPreventsAutomaticRestart() {
        CountingEngine engine = new CountingEngine();
        engine.onError(new RaftException(ErrorType.ERROR_TYPE_STATE_MACHINE));
        engine.restartRaftNode();
        engine.checkActivity();
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

    private static class CountingEngine extends PartitionEngine {

        private int shutdowns;
        private int starts;

        CountingEngine() {
            super(mock(HgStoreEngine.class), mock(ShardGroup.class));
        }

        @Override
        public Integer getGroupId() {
            return 1;
        }

        @Override
        public void shutdown() {
            this.shutdowns++;
        }

        @Override
        public synchronized boolean init(PartitionEngineOptions options) {
            this.starts++;
            return true;
        }
    }
}
