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

import org.apache.hugegraph.store.HeartbeatShutdownTest;
import org.apache.hugegraph.store.StoreIdChangeTest;
import org.apache.hugegraph.store.core.snapshot.HgSnapshotHandlerTest;
import org.apache.hugegraph.store.core.store.meta.PartitionMetaCodecTest;
import org.apache.hugegraph.store.node.AppConfigTuningTest;
import org.apache.hugegraph.store.node.listener.PdConfigureListenerMemoryOverrideTest;
import org.apache.hugegraph.store.options.CgroupMemoryLimitTest;
import org.apache.hugegraph.store.options.RaftRocksdbOptionsTest;
import org.apache.hugegraph.store.options.RocksdbMemoryBudgetTest;
import org.apache.hugegraph.store.util.FileObjectIteratorTest;
import org.apache.hugegraph.store.util.FilterIteratorCleanupTest;
import org.apache.hugegraph.store.util.FilterIteratorColumnEqualityTest;
import org.apache.hugegraph.store.util.SortShuffleSerializerTest;
import org.apache.hugegraph.store.util.SortShuffleTest;
import org.apache.hugegraph.store.util.TypeTransIteratorTest;
import org.apache.hugegraph.store.util.UnionFilterIteratorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import lombok.extern.slf4j.Slf4j;

// TODO: uncomment the rest of these classes once they can run free of each other.
//        HgCmdClientTest.class,
//        RaftUtilsTest.class,
//        RaftOperationTest.class,
//        UnsafeUtilTest.class,
//        CopyOnWriteCacheTest.class,
//        MetricServiceTest.class,
//        TaskManagerTest.class,
//        CleanTaskTest.class,
//        MetadataKeyHelperTest.class,
//        HgStoreEngineTest.class,
//        PartitionEngineTest.class,
//        PartitionManagerTest.class,
//        PartitionMetaStoreWrapperTest.class,
//        ZipUtilsTest.class,
//        MiscUtilClassTest.class,
//        PartitionInstructionProcessorTest.class,
//        // Try to put it last
//        HgBusinessImplTest.class
@RunWith(Suite.class)
@Suite.SuiteClasses({
        FileObjectIteratorTest.class,
        FilterIteratorCleanupTest.class,
        FilterIteratorColumnEqualityTest.class,
        SortShuffleSerializerTest.class,
        SortShuffleTest.class,
        TypeTransIteratorTest.class,
        UnionFilterIteratorTest.class,
        HeartbeatShutdownTest.class,
        MetricSessionTest.class,
        HgSnapshotHandlerTest.class,
        StoreIdChangeTest.class,
        PartitionMetaCodecTest.class,
        RocksdbMemoryBudgetTest.class,
        CgroupMemoryLimitTest.class,
        AppConfigTuningTest.class,
        RaftRocksdbOptionsTest.class,
        PdConfigureListenerMemoryOverrideTest.class
})
@Slf4j
public class CoreSuiteTest {

}
