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

package org.apache.hugegraph.unit;

import org.apache.hugegraph.core.RoleElectionStateMachineTest;
import org.apache.hugegraph.unit.api.filter.PathFilterTest;
import org.apache.hugegraph.unit.cache.CacheManagerTest;
import org.apache.hugegraph.unit.cache.CacheTest;
import org.apache.hugegraph.unit.cache.CachedGraphTransactionTest;
import org.apache.hugegraph.unit.cache.CachedSchemaTransactionTest;
import org.apache.hugegraph.unit.cache.RamTableTest;
import org.apache.hugegraph.unit.cassandra.CassandraTest;
import org.apache.hugegraph.unit.core.AnalyzerTest;
import org.apache.hugegraph.unit.core.BackendMutationTest;
import org.apache.hugegraph.unit.core.BackendStoreInfoTest;
import org.apache.hugegraph.unit.core.ConditionQueryFlattenTest;
import org.apache.hugegraph.unit.core.ConditionTest;
import org.apache.hugegraph.unit.core.DataTypeTest;
import org.apache.hugegraph.unit.core.DirectionsTest;
import org.apache.hugegraph.unit.core.ExceptionTest;
import org.apache.hugegraph.unit.core.LocksTableTest;
import org.apache.hugegraph.unit.core.PageStateTest;
import org.apache.hugegraph.unit.core.QueryTest;
import org.apache.hugegraph.unit.core.RangeTest;
import org.apache.hugegraph.unit.core.RolePermissionTest;
import org.apache.hugegraph.unit.core.RowLockTest;
import org.apache.hugegraph.unit.core.SecurityManagerTest;
import org.apache.hugegraph.unit.core.SerialEnumTest;
import org.apache.hugegraph.unit.core.SystemSchemaStoreTest;
import org.apache.hugegraph.unit.core.TraversalUtilTest;
import org.apache.hugegraph.unit.id.EdgeIdTest;
import org.apache.hugegraph.unit.id.IdTest;
import org.apache.hugegraph.unit.id.IdUtilTest;
import org.apache.hugegraph.unit.id.SplicingIdGeneratorTest;
import org.apache.hugegraph.unit.mysql.MysqlUtilTest;
import org.apache.hugegraph.unit.mysql.WhereBuilderTest;
import org.apache.hugegraph.unit.rocksdb.RocksDBCountersTest;
import org.apache.hugegraph.unit.rocksdb.RocksDBSessionTest;
import org.apache.hugegraph.unit.rocksdb.RocksDBSessionsTest;
import org.apache.hugegraph.unit.serializer.BinaryBackendEntryTest;
import org.apache.hugegraph.unit.serializer.BinaryScatterSerializerTest;
import org.apache.hugegraph.unit.serializer.BinarySerializerTest;
import org.apache.hugegraph.unit.serializer.BytesBufferTest;
import org.apache.hugegraph.unit.serializer.SerializerFactoryTest;
import org.apache.hugegraph.unit.serializer.StoreSerializerTest;
import org.apache.hugegraph.unit.serializer.TableBackendEntryTest;
import org.apache.hugegraph.unit.serializer.TextBackendEntryTest;
import org.apache.hugegraph.unit.store.RamIntObjectMapTest;
import org.apache.hugegraph.unit.util.CompressUtilTest;
import org.apache.hugegraph.unit.util.JsonUtilTest;
import org.apache.hugegraph.unit.util.RateLimiterTest;
import org.apache.hugegraph.unit.util.StringEncodingTest;
import org.apache.hugegraph.unit.util.VersionTest;
import org.apache.hugegraph.unit.util.collection.CollectionFactoryTest;
import org.apache.hugegraph.unit.util.collection.IdSetTest;
import org.apache.hugegraph.unit.util.collection.Int2IntsMapTest;
import org.apache.hugegraph.unit.util.collection.IntMapTest;
import org.apache.hugegraph.unit.util.collection.IntSetTest;
import org.apache.hugegraph.unit.util.collection.ObjectIntMappingTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        /* api filter */
        PathFilterTest.class,

        /* cache */
        CacheTest.RamCacheTest.class,
        CacheTest.OffheapCacheTest.class,
        CacheTest.LevelCacheTest.class,
        CachedSchemaTransactionTest.class,
        CachedGraphTransactionTest.class,
        CacheManagerTest.class,
        RamTableTest.class,

        /* types */
        DataTypeTest.class,
        DirectionsTest.class,
        SerialEnumTest.class,

        /* id */
        IdTest.class,
        EdgeIdTest.class,
        IdUtilTest.class,
        SplicingIdGeneratorTest.class,

        /* core */
        LocksTableTest.class,
        RowLockTest.class,
        AnalyzerTest.class,
        BackendMutationTest.class,
        ConditionTest.class,
        ConditionQueryFlattenTest.class,
        QueryTest.class,
        RangeTest.class,
        SecurityManagerTest.class,
        RolePermissionTest.class,
        ExceptionTest.class,
        BackendStoreInfoTest.class,
        TraversalUtilTest.class,
        PageStateTest.class,
        SystemSchemaStoreTest.class,
        RoleElectionStateMachineTest.class,

        /* serializer */
        BytesBufferTest.class,
        SerializerFactoryTest.class,
        TextBackendEntryTest.class,
        TableBackendEntryTest.class,
        BinaryBackendEntryTest.class,
        BinarySerializerTest.class,
        BinaryScatterSerializerTest.class,
        StoreSerializerTest.class,

        /* cassandra */
        CassandraTest.class,

        /* mysql */
        MysqlUtilTest.class,
        WhereBuilderTest.class,

        /* rocksdb */
        RocksDBSessionsTest.class,
        RocksDBSessionTest.class,
        RocksDBCountersTest.class,

        /* utils */
        VersionTest.class,
        JsonUtilTest.class,
        StringEncodingTest.class,
        CompressUtilTest.class,
        RateLimiterTest.FixedTimerWindowRateLimiterTest.class,
        RateLimiterTest.FixedWatchWindowRateLimiterTest.class,

        /* utils.collection */
        CollectionFactoryTest.class,
        ObjectIntMappingTest.class,
        Int2IntsMapTest.class,
        IdSetTest.class,
        IntMapTest.class,
        IntSetTest.class,

        /* store */
        RamIntObjectMapTest.class
})
public class UnitTestSuite {

}
