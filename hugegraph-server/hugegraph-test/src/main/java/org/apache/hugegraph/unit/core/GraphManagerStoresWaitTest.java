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

package org.apache.hugegraph.unit.core;

import java.lang.reflect.Constructor;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.core.GraphManager.Readiness;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

/**
 * The startup wait for a PD cluster (issue #3203): poll a readiness probe
 * until the cluster is ready, bounded by pd.stores_wait_timeout, with every
 * call's deadline bounded by the remaining budget.
 */
public class GraphManagerStoresWaitTest extends BaseUnitTest {

    private static Map.Entry<Readiness, String> answer(Readiness r, String m) {
        return new AbstractMap.SimpleImmutableEntry<>(r, m);
    }

    @Test
    public void testProbeAcceptsLargePartitionResponse() throws Exception {
        Metapb.Partition partition = Metapb.Partition.newBuilder()
                .setGraphName("graph-" + "x".repeat(120)).build();
        Pdpb.QueryPartitionsResponse response =
                Pdpb.QueryPartitionsResponse.newBuilder()
                    .addAllPartitions(Collections.nCopies(40000, partition))
                    .build();
        Assert.assertTrue(response.getSerializedSize() > 4 * 1024 * 1024);
        Server server = ServerBuilder.forPort(0)
                .addService(new PDGrpc.PDImplBase() {
                    @Override
                    public void queryPartitions(Pdpb.QueryPartitionsRequest request,
                                                StreamObserver<Pdpb.QueryPartitionsResponse> observer) {
                        observer.onNext(response);
                        observer.onCompleted();
                    }
                }).build().start();
        try {
            Class<?> clazz = Class.forName(
                    "org.apache.hugegraph.core.GraphManager$PdReadinessProbe");
            Constructor<?> constructor = clazz.getDeclaredConstructor(PDConfig.class);
            constructor.setAccessible(true);
            Object probe = constructor.newInstance(
                    PDConfig.of("127.0.0.1:" + server.getPort()));
            try (AutoCloseable closeable = (AutoCloseable) probe) {
                Map.Entry<Readiness, String> result =
                        ((GraphManager.ReadinessProbe) probe).probe(10000);
                Assert.assertEquals(result.getValue(), Readiness.READY, result.getKey());
            }
        } finally {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testInitialisedClusterIsNotWaitedFor() {
        // a restart with one store down: partitions exist, PD may even say
        // Cluster_Not_Ready, the server must not wait
        AtomicInteger calls = new AtomicInteger();
        long waited = GraphManager.waitForCluster(deadline -> {
            calls.incrementAndGet();
            return answer(Readiness.READY, "cluster already has 12 partition(s)");
        }, 300, 5);
        Assert.assertEquals(0L, waited);
        Assert.assertEquals(1, calls.get());
    }

    @Test
    public void testColdStartWaitsUntilPdReportsOk() {
        // first boot: stores register over time, PD flips to Cluster_OK on
        // the fourth poll; one unreachable answer in between is retried
        List<Map.Entry<Readiness, String>> answers = new ArrayList<>();
        answers.add(answer(Readiness.NOT_READY, "Cluster_Not_Ready: 0 stores"));
        answers.add(answer(Readiness.UNREACHABLE, "pd-1:8686: UNAVAILABLE"));
        answers.add(answer(Readiness.NOT_READY, "Cluster_Not_Ready: 1 store"));
        answers.add(answer(Readiness.READY, "PD reports Cluster_OK"));
        AtomicInteger calls = new AtomicInteger();
        long waited = GraphManager.waitForCluster(deadline -> {
            int i = Math.min(calls.getAndIncrement(), answers.size() - 1);
            return answers.get(i);
        }, 60, 1);
        Assert.assertEquals(4, calls.get());
        Assert.assertTrue("waited " + waited, waited >= 2 && waited <= 5);
    }

    @Test
    public void testBlackholedPdStaysWithinTheBudget() {
        // every call hangs for its whole deadline (a black-holed PD): the
        // deadline handed to the probe must shrink with the budget, and the
        // total wait must not exceed the timeout by more than one poll
        List<Long> deadlines = new ArrayList<>();
        long start = System.currentTimeMillis();
        Assert.assertThrows(HugeException.class, () -> {
            GraphManager.waitForCluster(deadline -> {
                deadlines.add(deadline);
                try {
                    Thread.sleep(deadline);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return answer(Readiness.UNREACHABLE, "pd:8686: DEADLINE_EXCEEDED");
            }, 3, 1);
        }, e -> {
            Assert.assertContains("Timed out after 3s", e.getMessage());
            Assert.assertContains("DEADLINE_EXCEEDED", e.getMessage());
            Assert.assertContains("pd.stores_wait_timeout", e.getMessage());
        });
        long elapsed = System.currentTimeMillis() - start;
        Assert.assertTrue("elapsed " + elapsed, elapsed < 5000);
        Assert.assertFalse(deadlines.isEmpty());
        for (long d : deadlines) {
            Assert.assertTrue("deadline " + d, d > 0 && d <= 1000);
        }
    }

    @Test
    public void testTimeoutKeepsPdsLastMessage() {
        Assert.assertThrows(HugeException.class, () -> {
            GraphManager.waitForCluster(deadline -> answer(
                    Readiness.NOT_READY,
                    "Cluster_Not_Ready: The number of active stores is 1, " +
                    "less than pd.initial-store-count:3"), 2, 1);
        }, e -> {
            Assert.assertContains("less than pd.initial-store-count:3",
                                  e.getMessage());
            Assert.assertContains("pd.stores_wait_timeout", e.getMessage());
        });
    }
}
