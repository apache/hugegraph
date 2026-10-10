/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.api.filter.AuthenticationFilter;
import org.apache.hugegraph.api.filter.PathFilter;
import org.apache.hugegraph.api.profile.StorageReadiness;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.UriInfo;

public class StorageReadinessTest {

    @Before
    @After
    public void reset() {
        StorageReadiness.resetCache();
    }

    private static Map<String, Object> result(boolean ready, String reason) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("ready", ready);
        map.put("reason", reason);
        map.put("active_stores", 3);
        return map;
    }

    @Test
    public void testReadyBodyAndCacheReuse() {
        AtomicInteger probes = new AtomicInteger();
        StorageReadiness.Probe probe = t -> {
            probes.incrementAndGet();
            return result(true, "ok");
        };
        Map<String, Object> first = StorageReadiness.check(probe, 1000L, 60_000L);
        Map<String, Object> second = StorageReadiness.check(probe, 1000L, 60_000L);
        Assert.assertTrue(StorageReadiness.isReady(first));
        Assert.assertEquals("hstore", first.get("storage"));
        Assert.assertEquals(false, first.get("cached"));
        Assert.assertEquals(true, second.get("cached"));
        Assert.assertEquals(1, probes.get());
    }

    @Test
    public void testZeroTtlProbesEveryTime() {
        AtomicInteger probes = new AtomicInteger();
        StorageReadiness.Probe probe = t -> {
            probes.incrementAndGet();
            return result(false, "no active store registered in pd");
        };
        StorageReadiness.check(probe, 1000L, 0L);
        Map<String, Object> body = StorageReadiness.check(probe, 1000L, 0L);
        Assert.assertFalse(StorageReadiness.isReady(body));
        Assert.assertEquals(2, probes.get());
        Assert.assertEquals("no active store registered in pd", body.get("reason"));
    }

    @Test
    public void testProbeFailureIsNotReadyWithReason() {
        StorageReadiness.Probe probe = t -> {
            throw new IllegalStateException("The 'hugegraph' store of hstore has not been opened");
        };
        Map<String, Object> body = StorageReadiness.check(probe, 1000L, 0L);
        Assert.assertFalse(StorageReadiness.isReady(body));
        Assert.assertEquals("probe failed: IllegalStateException", body.get("reason"));
        // the endpoint is unauthenticated: no raw message in the body
        Assert.assertFalse(body.toString().contains("has not been opened"));
    }

    @Test
    public void testTimeoutIsPassedToTheProbe() {
        StorageReadiness.Probe probe = t -> result(true, "budget " + t);
        Map<String, Object> body = StorageReadiness.check(probe, 750L, 0L);
        Assert.assertEquals("budget 750", body.get("reason"));
    }

    @Test
    public void testCachedCopyIsIsolated() {
        StorageReadiness.Probe probe = t -> result(true, "ok");
        Map<String, Object> first = StorageReadiness.check(probe, 1000L, 60_000L);
        first.put("ready", false);
        Map<String, Object> second = StorageReadiness.check(probe, 1000L, 60_000L);
        Assert.assertTrue(StorageReadiness.isReady(second));
    }

    /**
     * Concurrent callers with no cache share one probe: the first runs it on
     * its own thread, the others wait for that result. Nothing holds a
     * monitor while the probe does its I/O.
     */
    @Test
    public void testConcurrentCallersShareOneProbe() throws Exception {
        AtomicInteger probes = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        StorageReadiness.Probe probe = t -> {
            probes.incrementAndGet();
            started.countDown();
            release.await(5, TimeUnit.SECONDS);
            return result(true, "ok");
        };
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            Future<Map<String, Object>> owner = pool.submit(() -> {
                return StorageReadiness.check(probe, 2000L, 0L);
            });
            Assert.assertTrue(started.await(2, TimeUnit.SECONDS));
            List<Future<Map<String, Object>>> followers = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                followers.add(pool.submit(() -> StorageReadiness.check(probe, 2000L, 0L)));
            }
            Thread.sleep(100L);
            release.countDown();
            Assert.assertTrue(StorageReadiness.isReady(owner.get(2, TimeUnit.SECONDS)));
            Assert.assertEquals(false, owner.get().get("cached"));
            for (Future<Map<String, Object>> f : followers) {
                Map<String, Object> body = f.get(2, TimeUnit.SECONDS);
                Assert.assertTrue(StorageReadiness.isReady(body));
                Assert.assertEquals(true, body.get("shared"));
            }
            Assert.assertEquals(1, probes.get());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    /** Beyond maxWaiters, callers get an immediate 503 instead of a worker-pool slot. */
    @Test
    public void testExcessWaitersAreRejectedAtOnce() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        StorageReadiness.Probe probe = t -> {
            started.countDown();
            release.await(5, TimeUnit.SECONDS);
            return result(true, "ok");
        };
        ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            Future<Map<String, Object>> owner = pool.submit(() -> {
                return StorageReadiness.check("hstore", probe, 5000L, 0L, 1);
            });
            Assert.assertTrue(started.await(2, TimeUnit.SECONDS));
            Future<Map<String, Object>> waiter = pool.submit(() -> {
                return StorageReadiness.check("hstore", probe, 5000L, 0L, 1);
            });
            Thread.sleep(100L);
            long start = System.currentTimeMillis();
            Map<String, Object> rejected = StorageReadiness.check("hstore", probe, 5000L, 0L, 1);
            long took = System.currentTimeMillis() - start;
            Assert.assertFalse(StorageReadiness.isReady(rejected));
            Assert.assertEquals("too many readiness callers waiting for the probe (1)",
                                rejected.get("reason"));
            Assert.assertTrue("took " + took, took < 500L);
            release.countDown();
            Assert.assertTrue(StorageReadiness.isReady(owner.get(2, TimeUnit.SECONDS)));
            Map<String, Object> shared = waiter.get(2, TimeUnit.SECONDS);
            Assert.assertTrue(StorageReadiness.isReady(shared));
            Assert.assertEquals(true, shared.get("shared"));
            // the slot is free again
            Assert.assertTrue(StorageReadiness.isReady(
                    StorageReadiness.check("hstore", t -> result(true, "ok"), 1000L, 0L, 1)));
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    /** Independent backend configurations are probed side by side; one failing makes the server not ready. */
    @Test
    public void testEveryRemoteConfigurationIsProbed() throws Exception {
        AtomicInteger healthy = new AtomicInteger();
        AtomicInteger failing = new AtomicInteger();
        List<StorageReadiness.RemoteGraph> remotes = new ArrayList<>();
        remotes.add(new StorageReadiness.RemoteGraph("g1", "hbase", t -> {
            healthy.incrementAndGet();
            return result(true, "ok");
        }));
        remotes.add(new StorageReadiness.RemoteGraph("g2", "hbase", t -> {
            failing.incrementAndGet();
            return result(false, "hbase failed: IOException");
        }));
        Map<String, Object> body = StorageReadiness.probeAll(remotes, 1000L);
        Assert.assertEquals(1, healthy.get());
        Assert.assertEquals(1, failing.get());
        Assert.assertFalse(StorageReadiness.isReady(body));
        Assert.assertEquals("hbase configuration 2 of 2: hbase failed: IOException", body.get("reason"));
        List<?> probes = (List<?>) body.get("probes");
        Assert.assertEquals(2, probes.size());
        Assert.assertEquals(false, ((Map<?, ?>) probes.get(1)).get("ready"));
        // the unauthenticated body carries no graph names
        Assert.assertFalse(body.toString().contains("g2"));
        Assert.assertNull(((Map<?, ?>) probes.get(0)).get("graph"));

        // a hung configuration is bounded by the shared budget
        remotes.add(new StorageReadiness.RemoteGraph("g3", "hstore", t -> {
            Thread.sleep(5_000L);
            return result(true, "ok");
        }));
        long start = System.currentTimeMillis();
        body = StorageReadiness.probeAll(remotes, 300L);
        Assert.assertTrue(System.currentTimeMillis() - start < 2_000L);
        Assert.assertFalse(StorageReadiness.isReady(body));
        Assert.assertEquals(3, ((List<?>) body.get("probes")).size());

        // a single configuration keeps the plain body plus its one probe entry
        body = StorageReadiness.probeAll(remotes.subList(0, 1), 1000L);
        Assert.assertTrue(StorageReadiness.isReady(body));
        Assert.assertEquals(1, ((List<?>) body.get("probes")).size());
    }

    private static PropertiesConfiguration conf(Object... kv) {
        PropertiesConfiguration c = new PropertiesConfiguration();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            c.setProperty((String) kv[i], kv[i + 1]);
        }
        return c;
    }

    /** hbase graphs never share a probe (the namespace is per graph); hstore graphs share the PD cluster. */
    @Test
    public void testConfigKeys() {
        PropertiesConfiguration a = conf("hbase.hosts", "zk1,zk2", "hbase.port", 2181, "hbase.znode_parent", "/hbase");
        PropertiesConfiguration b = conf("hbase.hosts", "zk1,zk2", "hbase.port", 2181,
                                         "hbase.znode_parent", "/hbase-2");
        Assert.assertNotEquals(StorageReadiness.configKey("hbase", a, "g1"),
                               StorageReadiness.configKey("hbase", a, "g2"));
        Assert.assertNotEquals(StorageReadiness.configKey("hbase", a, "g1"),
                               StorageReadiness.configKey("hbase", b, "g1"));
        Assert.assertEquals(StorageReadiness.configKey("hbase", a, "g1"),
                            StorageReadiness.configKey("hbase", a, "g1"));
        PropertiesConfiguration p1 = conf("pd.peers", "pd:8686");
        PropertiesConfiguration p2 = conf("pd.peers", "pd2:8686");
        Assert.assertEquals(StorageReadiness.configKey("hstore", p1, "g1"),
                            StorageReadiness.configKey("hstore", p1, "g2"));
        Assert.assertNotEquals(StorageReadiness.configKey("hstore", p1, "g1"),
                               StorageReadiness.configKey("hstore", p2, "g1"));
    }

    /** A configured graph that failed to load makes the server not ready, remote or not. */
    @Test
    public void testFailedGraphLoadIsNotReady() {
        Map<String, Object> body = StorageReadiness.failedBody(2, "embedded");
        Assert.assertFalse(StorageReadiness.isReady(body));
        Assert.assertEquals("2 configured graph(s) failed to load", body.get("reason"));
        Assert.assertEquals(2, body.get("failed_graphs"));
        Assert.assertFalse(body.toString().contains("hugegraph"));
    }

    /** Every async probe runs as the internal admin: the auth context is a thread local. */
    @Test
    public void testAsyncProbesCarryTheAdminContext() throws Exception {
        List<StorageReadiness.RemoteGraph> remotes = new ArrayList<>();
        List<String> users = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < 3; i++) {
            remotes.add(new StorageReadiness.RemoteGraph("g" + i, "hstore", t -> {
                org.apache.hugegraph.auth.HugeGraphAuthProxy.Context ctx =
                        org.apache.hugegraph.auth.HugeGraphAuthProxy.getContext();
                users.add(ctx == null ? "none" : ctx.user().username());
                return result(true, "ok");
            }));
        }
        Map<String, Object> body = StorageReadiness.probeAll(remotes, 1000L);
        Assert.assertTrue(StorageReadiness.isReady(body));
        Assert.assertEquals(3, users.size());
        for (String u : users) {
            Assert.assertEquals("admin", u);
        }
    }

    /** The storage name in the body is the probed backend's. */
    @Test
    public void testStorageNameFollowsTheBackend() {
        Map<String, Object> body = StorageReadiness.check("hbase", t -> result(true, "ok"),
                                                          1000L, 0L, 4);
        Assert.assertEquals("hbase", body.get("storage"));
        StorageReadiness.resetCache();
        body = StorageReadiness.check("hbase", t -> {
            throw new IllegalStateException("x");
        }, 1000L, 0L, 4);
        Assert.assertEquals("hbase", body.get("storage"));
        Assert.assertFalse(StorageReadiness.isReady(body));
    }

    /** A follower waits at most its own timeout for the running probe. */
    @Test
    public void testFollowerWaitIsBounded() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        StorageReadiness.Probe probe = t -> {
            started.countDown();
            release.await(5, TimeUnit.SECONDS);
            return result(true, "ok");
        };
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Map<String, Object>> owner = pool.submit(() -> {
                return StorageReadiness.check(probe, 5000L, 0L);
            });
            Assert.assertTrue(started.await(2, TimeUnit.SECONDS));
            long start = System.currentTimeMillis();
            Map<String, Object> follower = StorageReadiness.check(probe, 200L, 0L);
            long took = System.currentTimeMillis() - start;
            Assert.assertFalse(StorageReadiness.isReady(follower));
            Assert.assertEquals("a probe is still running after 200 ms", follower.get("reason"));
            Assert.assertTrue("took " + took, took >= 200L && took < 2000L);
            release.countDown();
            Assert.assertTrue(StorageReadiness.isReady(owner.get(2, TimeUnit.SECONDS)));
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    /**
     * A Kubernetes httpGet probe carries no credential and no graphspace, so
     * the endpoint must pass both the graphspace path rewrite and the auth
     * filter, exactly like /versions.
     */
    @Test
    public void testReadinessBypassesPathAndAuthFilters() {
        Assert.assertTrue(PathFilter.isWhiteAPI("readiness"));
        Assert.assertTrue(PathFilter.isWhiteAPI("versions"));
        UriInfo uri = Mockito.mock(UriInfo.class);
        Mockito.when(uri.getPath()).thenReturn("readiness");
        ContainerRequestContext ctx = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(ctx.getUriInfo()).thenReturn(uri);
        Assert.assertTrue(AuthenticationFilter.isWhiteAPI(ctx));
        Mockito.when(uri.getPath()).thenReturn("readiness/");
        Assert.assertFalse(AuthenticationFilter.isWhiteAPI(ctx));
    }
}
