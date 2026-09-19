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

package org.apache.hugegraph.backend.store.hstore;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hugegraph.backend.store.hstore.HstoreStorageProbe.KnownStores;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

public class HstoreStorageProbeTest {

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
    private static final long BUDGET = 500L;

    @AfterClass
    public static void shutdown() {
        EXECUTOR.shutdownNow();
    }

    private static Metapb.Store store(long id) {
        return Metapb.Store.newBuilder().setId(id).setAddress("10.0.0." + id + ":8500")
                           .setState(Metapb.StoreState.Up).build();
    }

    private static List<Metapb.Store> stores(long... ids) {
        return Arrays.stream(ids).mapToObj(HstoreStorageProbeTest::store)
                     .collect(Collectors.toList());
    }

    private static KnownStores knowing(long... ids) {
        KnownStores known = new KnownStores();
        known.update(stores(ids));
        return known;
    }

    private static final HstoreStorageProbe.StorePinger ANSWERS = (store, timeout) -> {
    };

    private static String reason(Map<String, Object> body) {
        return (String) body.get("reason");
    }

    @Test
    public void testReadyWhenPdAndOneStoreAnswer() {
        Map<String, Object> r = HstoreStorageProbe.probe(new KnownStores(), () -> stores(1L, 2L, 3L),
                                            ANSWERS, BUDGET, EXECUTOR);
        Assert.assertTrue(reason(r), Boolean.TRUE.equals(r.get("ready")));
        Assert.assertEquals(3, r.get("active_stores"));
        Assert.assertNotNull(r.get("answered_store"));
        Assert.assertEquals(Boolean.TRUE, r.get("pd_reachable"));
        Assert.assertEquals("ok", reason(r));
    }

    @Test
    public void testFirstAnsweringStoreWinsAfterFailures() {
        AtomicInteger pings = new AtomicInteger();
        HstoreStorageProbe.StorePinger onlyThird = (store, timeout) -> {
            pings.incrementAndGet();
            if (store.getId() != 3L) {
                throw new IllegalStateException("UNAVAILABLE");
            }
        };
        Map<String, Object> r = HstoreStorageProbe.probe(knowing(1L, 2L, 3L), () -> stores(1L, 2L, 3L),
                                            onlyThird, BUDGET, EXECUTOR);
        Assert.assertTrue(reason(r), Boolean.TRUE.equals(r.get("ready")));
        Assert.assertEquals(3L, r.get("answered_store"));
        // the pings run in parallel; the failing ones may or may not have run
        Assert.assertTrue(pings.get() >= 1 && pings.get() <= 3);
    }

    /**
     * The store whose pod just went away hangs until its deadline; it must
     * not eat the budget of a store that answers (otherwise a rolling restart
     * would flap every server's readiness).
     */
    @Test
    public void testHungStoreDoesNotHideAnAnsweringOne() {
        HstoreStorageProbe.StorePinger onlySecondAnswers = (store, timeout) -> {
            if (store.getId() != 2L) {
                Thread.sleep(10_000L);
            }
        };
        long start = System.currentTimeMillis();
        Map<String, Object> r = HstoreStorageProbe.probe(knowing(1L, 2L, 3L), () -> stores(1L, 2L, 3L),
                                            onlySecondAnswers, BUDGET, EXECUTOR);
        long took = System.currentTimeMillis() - start;
        Assert.assertTrue(reason(r), Boolean.TRUE.equals(r.get("ready")));
        Assert.assertEquals(2L, r.get("answered_store"));
        Assert.assertTrue("took " + took, took < BUDGET);
    }

    @Test
    public void testFirstProbeWithoutKnownStoresNeedsPd() {
        Map<String, Object> r = HstoreStorageProbe.probe(new KnownStores(), () -> {
            throw new IllegalStateException("UNAVAILABLE: io exception");
        }, ANSWERS, BUDGET, EXECUTOR);
        Assert.assertFalse(Boolean.TRUE.equals(r.get("ready")));
        Assert.assertTrue(reason(r), reason(r).startsWith(
                "no store list known and pd failed: IllegalStateException"));
        Assert.assertEquals(Boolean.FALSE, r.get("pd_reachable"));
    }

    @Test
    public void testFirstProbeWithHungPdStaysWithinBudget() {
        long start = System.currentTimeMillis();
        Map<String, Object> r = HstoreStorageProbe.probe(new KnownStores(), () -> {
            Thread.sleep(10_000L);
            return stores(1L);
        }, ANSWERS, BUDGET, EXECUTOR);
        long took = System.currentTimeMillis() - start;
        Assert.assertFalse(Boolean.TRUE.equals(r.get("ready")));
        Assert.assertTrue(reason(r), reason(r).contains("pd did not answer within"));
        Assert.assertTrue("took " + took, took < BUDGET * 4);
    }

    /**
     * PD down, restarting or slow must not change the readiness of a server
     * whose stores still answer: the last known list is used right away.
     */
    @Test
    public void testKnownStoresKeepTheServerReadyWhilePdIsDown() {
        Map<String, Object> r = HstoreStorageProbe.probe(knowing(1L, 2L), () -> {
            throw new IllegalStateException("PD unreachable");
        }, ANSWERS, BUDGET, EXECUTOR);
        Assert.assertTrue(reason(r), Boolean.TRUE.equals(r.get("ready")));
        Assert.assertEquals(2, r.get("active_stores"));
    }

    @Test
    public void testHungPdDoesNotDelayAProbeWithKnownStores() {
        long start = System.currentTimeMillis();
        Map<String, Object> r = HstoreStorageProbe.probe(knowing(1L, 2L), () -> {
            Thread.sleep(10_000L);
            return stores(1L, 2L);
        }, ANSWERS, BUDGET, EXECUTOR);
        long took = System.currentTimeMillis() - start;
        Assert.assertTrue(reason(r), Boolean.TRUE.equals(r.get("ready")));
        // the refresh is still pending, so the outcome of the last finished
        // one (the seed) is reported
        Assert.assertEquals(Boolean.TRUE, r.get("pd_reachable"));
        Assert.assertTrue("took " + took, took < BUDGET);
    }

    @Test
    public void testLastPdOutcomeIsReportedWhileTheRefreshIsPending() throws Exception {
        KnownStores known = knowing(1L);
        HstoreStorageProbe.probe(known, () -> {
            throw new IllegalStateException("PD unreachable");
        }, ANSWERS, BUDGET, EXECUTOR);
        for (int i = 0; i < 50 && known.pdOk() == null; i++) {
            Thread.sleep(20L);
        }
        Assert.assertEquals(Boolean.FALSE, known.pdOk());
        Map<String, Object> r = HstoreStorageProbe.probe(known, () -> {
            Thread.sleep(10_000L);
            return stores(1L);
        }, ANSWERS, BUDGET, EXECUTOR);
        Assert.assertTrue(Boolean.TRUE.equals(r.get("ready")));
        Assert.assertEquals(Boolean.FALSE, r.get("pd_reachable"));
        Assert.assertTrue(r.containsKey("pd_checked_age_ms"));
    }

    @Test
    public void testPdAnswerUpdatesTheKnownStoresForTheNextProbe() throws Exception {
        KnownStores known = knowing(1L);
        HstoreStorageProbe.probe(known, () -> stores(1L, 2L, 3L), ANSWERS, BUDGET, EXECUTOR);
        for (int i = 0; i < 50 && known.stores().size() != 3; i++) {
            Thread.sleep(20L);
        }
        Assert.assertEquals(3, known.stores().size());
        Assert.assertTrue(known.ageMs() >= 0L);
    }

    @Test
    public void testEmptyPdAnswerIsNotReadyAndKeepsTheOldList() {
        KnownStores fresh = new KnownStores();
        Map<String, Object> r = HstoreStorageProbe.probe(fresh, Collections::emptyList, ANSWERS,
                                            BUDGET, EXECUTOR);
        Assert.assertFalse(Boolean.TRUE.equals(r.get("ready")));
        Assert.assertEquals("no active store registered in pd", reason(r));
        KnownStores known = knowing(1L);
        HstoreStorageProbe.probe(known, Collections::emptyList, ANSWERS, BUDGET, EXECUTOR);
        Assert.assertEquals(1, known.stores().size());
    }

    @Test
    public void testNotReadyWhenEveryStoreFails() {
        HstoreStorageProbe.StorePinger refused = (store, timeout) -> {
            throw new IllegalStateException("connection refused");
        };
        Map<String, Object> r = HstoreStorageProbe.probe(knowing(7L, 8L), () -> stores(7L, 8L),
                                            refused, BUDGET, EXECUTOR);
        Assert.assertFalse(Boolean.TRUE.equals(r.get("ready")));
        Assert.assertTrue(reason(r), reason(r).startsWith("none of 2 known store(s) answered"));
        Assert.assertTrue(reason(r), reason(r).contains("a store failed: IllegalStateException"));
        Assert.assertFalse(reason(r), reason(r).contains("connection refused"));
        Assert.assertNull(r.get("answered_store"));
    }

    @Test
    public void testHungStoresStayWithinTheBudget() {
        HstoreStorageProbe.StorePinger hung = (store, timeout) -> {
            Thread.sleep(10_000L);
        };
        long start = System.currentTimeMillis();
        Map<String, Object> r = HstoreStorageProbe.probe(knowing(1L, 2L, 3L), () -> stores(1L, 2L, 3L),
                                            hung, BUDGET, EXECUTOR);
        long took = System.currentTimeMillis() - start;
        Assert.assertFalse(Boolean.TRUE.equals(r.get("ready")));
        Assert.assertTrue(reason(r), reason(r).contains("did not answer within"));
        Assert.assertTrue("took " + took, took < BUDGET * 4);
    }

    @Test
    public void testPingGetsTheRemainingBudget() {
        List<Long> budgets = Collections.synchronizedList(new java.util.ArrayList<>());
        HstoreStorageProbe.probe(knowing(1L), () -> stores(1L), (store, timeout) -> {
            budgets.add(timeout);
        }, BUDGET, EXECUTOR);
        Assert.assertEquals(1, budgets.size());
        Assert.assertTrue(budgets.get(0) > 0L && budgets.get(0) <= BUDGET);
    }

    @Test
    public void testMapCarriesNoAddresses() {
        Map<String, Object> map = HstoreStorageProbe.probe(knowing(1L), () -> stores(1L),
                                                           ANSWERS, BUDGET, EXECUTOR);
        Assert.assertEquals(true, map.get("ready"));
        Assert.assertEquals(1, map.get("active_stores"));
        Assert.assertEquals(1L, map.get("answered_store"));
        Assert.assertTrue(map.containsKey("pd_reachable"));
        Assert.assertTrue(map.containsKey("stores_age_ms"));
        Assert.assertFalse(map.toString().contains("10.0.0."));
    }

    @Test
    public void testRejectsNonPositiveBudget() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            HstoreStorageProbe.probe(new KnownStores(), Collections::emptyList, ANSWERS,
                                     0L, EXECUTOR);
        });
    }

    /**
     * The body is served without authentication: PD peers from the PD client's
     * "PD unreachable, pd.peers=..." and Store host names from gRPC's "Unable
     * to resolve host ..." must not reach it, only a category.
     */
    @Test
    public void testReasonCarriesNoPdPeersNorStoreHosts() {
        Map<String, Object> pd = HstoreStorageProbe.probe(new KnownStores(), () -> {
            throw new PDException(1, "PD unreachable, pd.peers=pd-0.internal:8686,pd-1.internal:8686");
        }, ANSWERS, BUDGET, EXECUTOR);
        Assert.assertFalse(Boolean.TRUE.equals(pd.get("ready")));
        Assert.assertEquals("no store list known and pd failed: pd unreachable", reason(pd));

        HstoreStorageProbe.StorePinger unresolved = (store, timeout) -> {
            throw Status.UNAVAILABLE.withDescription(
                    "Unable to resolve host store-0.hugegraph-store.svc").asRuntimeException();
        };
        Map<String, Object> st = HstoreStorageProbe.probe(knowing(1L), () -> stores(1L), unresolved,
                                             BUDGET, EXECUTOR);
        Assert.assertFalse(Boolean.TRUE.equals(st.get("ready")));
        Assert.assertTrue(reason(st), reason(st).contains("a store failed: UNAVAILABLE"));
        String all = pd.toString() + st.toString();
        Assert.assertFalse(all, all.contains("internal") || all.contains("svc") ||
                                all.contains("8686"));
    }

    /** A hung PD parks one refresh, not one per probe. */
    @Test
    public void testRefreshIsSingleFlight() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        KnownStores known = knowing(1L);
        HstoreStorageProbe.StoreLister hung = () -> {
            calls.incrementAndGet();
            Thread.sleep(3_000L);
            return stores(1L, 2L);
        };
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(Boolean.TRUE, HstoreStorageProbe.probe(known, hung, ANSWERS,
                                                                       BUDGET, EXECUTOR)
                                                                .get("ready"));
        }
        Thread.sleep(200L);
        Assert.assertEquals(1, calls.get());
        for (int i = 0; i < 40 && known.stores().size() != 2; i++) {
            Thread.sleep(100L);
        }
        Assert.assertEquals(2, known.stores().size());
        HstoreStorageProbe.probe(known, hung, ANSWERS, BUDGET, EXECUTOR);
        Thread.sleep(100L);
        Assert.assertEquals("a finished refresh allows a new one", 2, calls.get());
    }

    private static final class FakeChannel extends ManagedChannel {

        boolean shut;

        @Override
        public ManagedChannel shutdown() {
            this.shut = true;
            return this;
        }

        @Override
        public boolean isShutdown() {
            return this.shut;
        }

        @Override
        public boolean isTerminated() {
            return this.shut;
        }

        @Override
        public ManagedChannel shutdownNow() {
            return this.shutdown();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public <Q, P> ClientCall<Q, P> newCall(MethodDescriptor<Q, P> method,
                                               CallOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String authority() {
            return "fake";
        }
    }

    @Test
    public void testChannelsOfReplacedStoresAreShutDown() {
        Map<String, ManagedChannel> channels = new java.util.concurrent.ConcurrentHashMap<>();
        FakeChannel kept = new FakeChannel();
        FakeChannel gone = new FakeChannel();
        channels.put("10.0.0.1:8500", kept);
        channels.put("10.0.0.9:8500", gone);
        HstoreStorageProbe.pruneChannels(channels, stores(1L, 2L));
        Assert.assertEquals(1, channels.size());
        Assert.assertFalse(kept.shut);
        Assert.assertTrue(gone.shut);
        HstoreStorageProbe.pruneChannels(channels, null);
        Assert.assertEquals("a failed listing prunes nothing", 1, channels.size());
        HstoreStorageProbe.pruneChannels(channels, Collections.emptyList());
        Assert.assertEquals("an empty listing keeps the channels the pings still use",
                            1, channels.size());
        Assert.assertFalse(kept.shut);
    }
}
