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

package org.apache.hugegraph.unit.cassandra;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.backend.store.cassandra.CassandraOptions;
import org.apache.hugegraph.backend.store.cassandra.CassandraSessionPool;
import org.apache.hugegraph.backend.store.cassandra.CassandraStore;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CassandraTest {

    @Before
    public void setup() {
        OptionSpace.register("cassandra",
                             "org.apache.hugegraph.backend.store.cassandra.CassandraOptions");
    }

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testParseReplicaWithSimpleStrategy() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(strategy, "SimpleStrategy");
        conf.setProperty(replica, ImmutableList.of("5"));
        HugeConfig config = new HugeConfig(conf);

        Map<String, Object> result = Whitebox.invokeStatic(CassandraStore.class,
                                                           "parseReplica",
                                                           config);

        Map<String, Object> expected = ImmutableMap.of(
                "class", "SimpleStrategy",
                "replication_factor", 5);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testParseReplicaWithNetworkTopologyStrategy() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(strategy, "NetworkTopologyStrategy");
        conf.setProperty(replica, ImmutableList.of("dc1:2", "dc2:1"));
        HugeConfig config = new HugeConfig(conf);

        Map<String, Object> result = Whitebox.invokeStatic(CassandraStore.class,
                                                           "parseReplica",
                                                           config);

        Map<String, Object> expected = ImmutableMap.of(
                "class", "NetworkTopologyStrategy",
                "dc1", 2,
                "dc2", 1);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testParseReplicaWithSimpleStrategyAndEmptyReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(strategy, "SimpleStrategy");
        conf.setProperty(replica, ImmutableList.of(""));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseReplicaWithSimpleStrategyAndDoubleReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(strategy, "SimpleStrategy");
        conf.setProperty(replica, ImmutableList.of("1.5"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseReplicaWithSimpleStrategyAndStringReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(strategy, "SimpleStrategy");
        conf.setProperty(replica, ImmutableList.of("string"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseReplicaWithNetworkTopologyStrategyAndStringReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(strategy, "NetworkTopologyStrategy");
        conf.setProperty(replica, ImmutableList.of("dc1:2", "dc2:string"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseReplicaWithNetworkTopologyStrategyWithoutDatacenter() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(strategy, "NetworkTopologyStrategy");
        conf.setProperty(replica, ImmutableList.of(":2", "dc2:1"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseReplicaWithNetworkTopologyStrategyAndEmptyReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(strategy, "NetworkTopologyStrategy");
        conf.setProperty(replica, ImmutableList.of("dc1:", "dc2:1"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseReplicaWithNetworkTopologyStrategyAndDoubleReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(strategy, "NetworkTopologyStrategy");
        conf.setProperty(replica, ImmutableList.of("dc1:3.5", "dc2:1"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testReconnectOptionsHaveSensibleDefaults() {
        // Runtime-reconnection options must exist with non-zero defaults so
        // HugeGraph keeps running when Cassandra restarts (issue #2740).
        Assert.assertEquals(1000L, (long) CassandraOptions
                .CASSANDRA_RECONNECT_BASE_DELAY.defaultValue());
        Assert.assertEquals(10_000L, (long) CassandraOptions
                .CASSANDRA_RECONNECT_MAX_DELAY.defaultValue());
        Assert.assertEquals(3, (int) CassandraOptions
                .CASSANDRA_RECONNECT_MAX_RETRIES.defaultValue());
        Assert.assertEquals(1000L, (long) CassandraOptions
                .CASSANDRA_RECONNECT_INTERVAL.defaultValue());
    }

    @Test
    public void testReconnectOptionsAreOverridable() {
        String base = CassandraOptions.CASSANDRA_RECONNECT_BASE_DELAY.name();
        String max = CassandraOptions.CASSANDRA_RECONNECT_MAX_DELAY.name();
        String retries = CassandraOptions.CASSANDRA_RECONNECT_MAX_RETRIES
                                         .name();
        String interval = CassandraOptions.CASSANDRA_RECONNECT_INTERVAL.name();

        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(base, 500L);
        conf.setProperty(max, 30_000L);
        conf.setProperty(retries, 3);
        conf.setProperty(interval, 1000L);
        HugeConfig config = new HugeConfig(conf);

        Assert.assertEquals(500L, (long) config.get(
                CassandraOptions.CASSANDRA_RECONNECT_BASE_DELAY));
        Assert.assertEquals(30_000L, (long) config.get(
                CassandraOptions.CASSANDRA_RECONNECT_MAX_DELAY));
        Assert.assertEquals(3, (int) config.get(
                CassandraOptions.CASSANDRA_RECONNECT_MAX_RETRIES));
        Assert.assertEquals(1000L, (long) config.get(
                CassandraOptions.CASSANDRA_RECONNECT_INTERVAL));
    }

    @Test
    public void testReconnectRetriesCanBeDisabled() {
        String retries = CassandraOptions.CASSANDRA_RECONNECT_MAX_RETRIES
                                         .name();
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(retries, 0);
        HugeConfig config = new HugeConfig(conf);
        Assert.assertEquals(0, (int) config.get(
                CassandraOptions.CASSANDRA_RECONNECT_MAX_RETRIES));
    }

    @Test
    public void testExecuteWithRetrySucceedsAfterTransientFailures() {
        // Configure retry knobs via config so the pool reads them through
        // the normal path (no Whitebox overrides on retry fields). Keep the
        // values within the validators' lower bounds (base >= 100, max >=
        // base, interval >= 100).
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(
                CassandraOptions.CASSANDRA_RECONNECT_BASE_DELAY.name(), 100L);
        conf.setProperty(
                CassandraOptions.CASSANDRA_RECONNECT_MAX_DELAY.name(), 1000L);
        conf.setProperty(
                CassandraOptions.CASSANDRA_RECONNECT_MAX_RETRIES.name(), 3);
        conf.setProperty(
                CassandraOptions.CASSANDRA_RECONNECT_INTERVAL.name(), 100L);
        HugeConfig config = new HugeConfig(conf);
        CassandraSessionPool pool = new CassandraSessionPool(config,
                                                             "ks", "store");

        com.datastax.driver.core.Session driverSession = Mockito.mock(
                com.datastax.driver.core.Session.class);
        ResultSet rs = Mockito.mock(ResultSet.class);
        NoHostAvailableException transientFailure =
                new NoHostAvailableException(Collections.emptyMap());
        Mockito.when(driverSession.execute(Mockito.any(Statement.class)))
               .thenThrow(transientFailure)
               .thenThrow(transientFailure)
               .thenReturn(rs);

        // executeWithRetry now resets the driver session on transient
        // failures, so the next iteration calls cluster().connect(keyspace)
        // to obtain a fresh one. Install a mocked Cluster that hands back
        // the same driverSession for each reconnect.
        com.datastax.driver.core.Cluster mockCluster = Mockito.mock(
                com.datastax.driver.core.Cluster.class);
        Mockito.when(mockCluster.isClosed()).thenReturn(false);
        Mockito.when(mockCluster.connect(Mockito.anyString()))
               .thenReturn(driverSession);
        Whitebox.setInternalState(pool, "cluster", mockCluster);

        CassandraSessionPool.Session session = pool.new Session();
        Whitebox.setInternalState(session, "session", driverSession);

        ResultSet result = session.execute("SELECT now() FROM system.local");
        Assert.assertSame(rs, result);
        Mockito.verify(driverSession, Mockito.times(3))
               .execute(Mockito.any(Statement.class));
    }

    @Test
    public void testReconnectBaseDelayBelowMinimumRejected() {
        // The validator on CASSANDRA_RECONNECT_BASE_DELAY is
        // rangeInt(100L, Long.MAX_VALUE); values below 100 must be rejected
        // at parse time. Setting the property as a String forces HugeConfig
        // to run parseConvert() which invokes the range check.
        Configuration conf = new PropertiesConfiguration();
        Assert.assertThrows(Exception.class, () -> {
            conf.setProperty(
                    CassandraOptions.CASSANDRA_RECONNECT_BASE_DELAY.name(),
                    "50");
            new HugeConfig(conf);
        });
    }

    @Test
    public void testReconnectMaxDelayLessThanBaseRejected() {
        // Both values must pass their individual range validators with margin
        // (base >= 100, max >= 1000), so the only thing that can throw is the
        // E.checkArgument(max >= base) cross-check inside the pool ctor. Set
        // all four reconnect properties explicitly so the test does not depend
        // on default values changing in CassandraOptions.
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(
                CassandraOptions.CASSANDRA_RECONNECT_BASE_DELAY.name(), 10_000L);
        conf.setProperty(
                CassandraOptions.CASSANDRA_RECONNECT_MAX_DELAY.name(), 2_000L);
        conf.setProperty(
                CassandraOptions.CASSANDRA_RECONNECT_MAX_RETRIES.name(), 3);
        conf.setProperty(
                CassandraOptions.CASSANDRA_RECONNECT_INTERVAL.name(), 1_000L);
        HugeConfig config = new HugeConfig(conf);
        Assert.assertThrows(IllegalArgumentException.class, () ->
                new CassandraSessionPool(config, "ks", "store"));
    }
}
