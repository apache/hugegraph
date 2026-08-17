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

package org.apache.hugegraph.pd.raft.auth;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IpAuthHandlerTest {

    @Before
    public void setUp() {
        // Must reset BEFORE each test — earlier suite classes (e.g. ConfigServiceTest)
        // initialize RaftEngine which creates the IpAuthHandler singleton with their
        // own peer IPs. Without this reset, our getInstance() calls return the stale
        // singleton and ignore the allowlist passed by the test.
        IpAuthHandler.shutdownInstance();
    }

    @After
    public void tearDown() {
        // Must reset AFTER each test — prevents our test singleton from leaking
        // into later suite classes that also depend on IpAuthHandler state.
        IpAuthHandler handler = IpAuthHandler.getInstance();
        if (handler != null) {
            IpAuthHandler.shutdownInstance();
        }
    }

    private boolean isIpAllowed(IpAuthHandler handler, String ip) {
        return Whitebox.invoke(IpAuthHandler.class,
                               new Class[]{String.class},
                               "isIpAllowed", handler, ip);
    }

    @Test
    public void testHostnameResolvesToIp() throws Exception {
        // "localhost" should resolve to one or more IPs via InetAddress.getAllByName()
        // This verifies the core fix: hostname allowlists match numeric remote addresses
        // Using dynamic resolution avoids hardcoding "127.0.0.1" which may not be
        // returned on IPv6-only or custom resolver environments
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("localhost"));
        InetAddress[] addresses = InetAddress.getAllByName("localhost");
        Assert.assertTrue("Expected at least one resolved address",
                          addresses.length > 0);
        boolean matched = false;
        for (InetAddress address : addresses) {
            matched |= isIpAllowed(handler, address.getHostAddress());
        }
        Assert.assertTrue("Expected a resolved address to be allowed", matched);
    }

    @Test
    public void testTransientDnsFailureRecoversOnRefresh() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        InetAddress expected = InetAddress.getByAddress(
                new byte[]{(byte) 192, (byte) 168, 0, 1});
        IpAuthHandler handler = new IpAuthHandler(
                Collections.singleton("pd-1"),
                host -> {
                    if (attempts.incrementAndGet() < 3) {
                        return failed(host);
                    }
                    return resolved(expected);
                },
                false, 100L, 1_000L, 1_000L);

        Assert.assertFalse(isIpAllowed(handler, expected.getHostAddress()));
        handler.refreshResolvedIps();
        handler.refreshResolvedIps();
        Assert.assertTrue(isIpAllowed(handler, expected.getHostAddress()));
        Assert.assertEquals(3, attempts.get());
        handler.shutdown();
    }

    @Test
    public void testTransientDnsFailureKeepsLastKnownAddress() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        InetAddress expected = InetAddress.getByAddress(
                new byte[]{(byte) 192, (byte) 168, 0, 1});
        IpAuthHandler handler = new IpAuthHandler(
                Collections.singleton("pd-1"),
                host -> {
                    if (attempts.incrementAndGet() > 1) {
                        return failed(host);
                    }
                    return resolved(expected);
                },
                false, 100L, 1_000L, 1_000L);

        Assert.assertTrue(isIpAllowed(handler, expected.getHostAddress()));
        handler.refreshResolvedIps();
        Assert.assertTrue(isIpAllowed(handler, expected.getHostAddress()));
        handler.shutdown();
    }

    @Test
    public void testSlowPeerDoesNotBlockFollowingPeer() throws Exception {
        InetAddress expected = InetAddress.getByAddress(
                new byte[]{(byte) 192, (byte) 168, 0, 2});
        Set<String> peers = new LinkedHashSet<>();
        peers.add("pd-slow");
        peers.add("pd-ready");
        IpAuthHandler handler = new IpAuthHandler(
                peers,
                host -> {
                    if ("pd-slow".equals(host)) {
                        return new CompletableFuture<>();
                    }
                    return resolved(expected);
                },
                false, 10L, 1_000L, 1_000L);

        Assert.assertTrue(isIpAllowed(handler, expected.getHostAddress()));
        handler.shutdown();
    }

    @Test
    public void testExpiredAddressFailsClosed() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        InetAddress expected = InetAddress.getByAddress(
                new byte[]{(byte) 192, (byte) 168, 0, 3});
        IpAuthHandler handler = new IpAuthHandler(
                Collections.singleton("pd-1"),
                host -> {
                    if (attempts.incrementAndGet() > 1) {
                        return failed(host);
                    }
                    return resolved(expected);
                },
                false, 100L, 1L, 1_000L);

        Assert.assertTrue(isIpAllowed(handler, expected.getHostAddress()));
        Thread.sleep(5L);
        handler.refreshResolvedIps();
        Assert.assertFalse(isIpAllowed(handler, expected.getHostAddress()));
        handler.shutdown();
    }

    @Test
    public void testScheduledRefreshAddsLatePeerAndRotatesAddress()
            throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        InetAddress first = InetAddress.getByAddress(
                new byte[]{(byte) 192, (byte) 168, 0, 4});
        InetAddress second = InetAddress.getByAddress(
                new byte[]{(byte) 192, (byte) 168, 0, 5});
        AtomicReference<InetAddress> current = new AtomicReference<>(first);
        IpAuthHandler handler = new IpAuthHandler(
                Collections.singleton("pd-late"),
                host -> {
                    if (attempts.incrementAndGet() == 1) {
                        return failed(host);
                    }
                    return resolved(current.get());
                },
                true, 20L, 1_000L, 10L);
        try {
            awaitAllowed(handler, first.getHostAddress());
            current.set(second);
            awaitAllowed(handler, second.getHostAddress());
            Assert.assertFalse(isIpAllowed(handler, first.getHostAddress()));
        } finally {
            handler.shutdown();
        }
    }

    @Test
    public void testNeverCompletingPeersDoNotStarveReadyPeer()
            throws Exception {
        InetAddress expected = InetAddress.getByAddress(
                new byte[]{(byte) 192, (byte) 168, 0, 6});
        Set<String> peers = new LinkedHashSet<>();
        for (int i = 0; i < 8; i++) {
            peers.add("00-pd-slow-" + i);
        }
        peers.add("99-pd-ready");
        IpAuthHandler handler = new IpAuthHandler(
                peers,
                host -> {
                    if (host.startsWith("00-pd-slow-")) {
                        return new CompletableFuture<>();
                    }
                    return resolved(expected);
                },
                false, 10L, 1_000L, 1_000L);

        handler.refresh(peers);
        Assert.assertTrue(isIpAllowed(handler, expected.getHostAddress()));
        handler.shutdown();
    }

    @Test
    public void testRejectsOversizedAllowlist() {
        Set<String> peers = new HashSet<>();
        for (int i = 0; i < 128; i++) {
            peers.add("pd-" + i);
        }

        try {
            new IpAuthHandler(peers, host -> new CompletableFuture<>(),
                              false, 10L, 1_000L, 1_000L);
            Assert.fail("Expected oversized allowlist rejection");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("127"));
        }
    }

    @Test
    public void testLateSuccessfulResultIsDiscarded() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        InetAddress first = InetAddress.getByAddress(
                new byte[]{(byte) 192, (byte) 168, 0, 7});
        InetAddress late = InetAddress.getByAddress(
                new byte[]{(byte) 192, (byte) 168, 0, 8});
        CompletableFuture<Set<String>> delayed = new CompletableFuture<>();
        IpAuthHandler handler = new IpAuthHandler(
                Collections.singleton("pd-1"),
                host -> {
                    int attempt = attempts.incrementAndGet();
                    if (attempt == 1) {
                        return resolved(first);
                    }
                    if (attempt == 2) {
                        return delayed;
                    }
                    return new CompletableFuture<>();
                },
                false, 10L, 1_000L, 1_000L);

        handler.refreshResolvedIps(false);
        Thread.sleep(20L);
        delayed.complete(resolved(late).get());
        handler.refreshResolvedIps(false);

        Assert.assertTrue(isIpAllowed(handler, first.getHostAddress()));
        Assert.assertFalse(isIpAllowed(handler, late.getHostAddress()));
        handler.shutdown();
    }

    @Test
    public void testRefreshCollectsPreviousBatchBeforeStartingNext()
            throws Exception {
        Set<String> peers = new HashSet<>();
        Map<Integer, CompletableFuture<Set<String>>> delayed =
                new HashMap<>();
        for (int i = 0; i < 17; i++) {
            peers.add(String.format("pd-%02d", i));
            if (i >= 8 && i < 16) {
                delayed.put(i, new CompletableFuture<>());
            }
        }
        IpAuthHandler handler = new IpAuthHandler(
                peers,
                host -> {
                    int index = Integer.parseInt(host.substring(3));
                    CompletableFuture<Set<String>> future = delayed.get(index);
                    if (future != null) {
                        return future;
                    }
                    return resolved(address(index));
                },
                false, 100L, 1_000L, 1_000L);

        handler.refreshResolvedIps(false);
        for (Map.Entry<Integer, CompletableFuture<Set<String>>> entry :
                delayed.entrySet()) {
            entry.getValue().complete(resolved(address(entry.getKey())).get());
        }
        handler.refreshResolvedIps(false);

        Assert.assertTrue(isIpAllowed(
                handler, address(16).getHostAddress()));
        handler.shutdown();
    }

    @Test
    public void testConstructorFailureClosesResolver() {
        AtomicBoolean closed = new AtomicBoolean();
        IpAuthHandler.HostResolver resolver = new IpAuthHandler.HostResolver() {

            @Override
            public CompletableFuture<Set<String>> resolve(String host) {
                throw new IllegalStateException("simulated resolver failure");
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        try {
            new IpAuthHandler(Collections.singleton("pd-1"), resolver,
                              false, 10L, 1_000L, 1_000L);
            Assert.fail("Expected constructor failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("simulated resolver failure", e.getMessage());
        }
        Assert.assertTrue(closed.get());
    }

    @Test
    public void testInterruptedRefreshFailsAndPreservesInterrupt()
            throws Exception {
        InetAddress initial = address(20);
        IpAuthHandler handler = new IpAuthHandler(
                Collections.singleton("ready"),
                host -> {
                    if ("ready".equals(host)) {
                        return resolved(initial);
                    }
                    return new CompletableFuture<>();
                },
                false, 100L, 1_000L, 1_000L);
        try {
            Thread.currentThread().interrupt();
            handler.refresh(Collections.singleton("slow"));
            Assert.fail("Expected interrupted refresh to fail");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("interrupted"));
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            handler.shutdown();
        }
    }

    private void awaitAllowed(IpAuthHandler handler, String address)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 1_000L;
        while (!isIpAllowed(handler, address) &&
               System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        Assert.assertTrue(isIpAllowed(handler, address));
    }

    @Test
    public void testRefreshUpdatesResolvedIps() {
        // Start with 127.0.0.1
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("127.0.0.1"));
        Assert.assertTrue(isIpAllowed(handler, "127.0.0.1"));

        // Refresh with a different IP — verifies refresh() swaps the set correctly
        Set<String> newIps = new HashSet<>();
        newIps.add("192.168.0.1");
        handler.refresh(newIps);

        // Old IP should no longer be allowed
        Assert.assertFalse(isIpAllowed(handler, "127.0.0.1"));
        // New IP should now be allowed
        Assert.assertTrue(isIpAllowed(handler, "192.168.0.1"));
    }

    @Test
    public void testEmptyAllowlistAllowsAll() {
        // Empty allowlist = no restriction configured = allow all connections
        // This is intentional fallback behavior and must be explicitly tested
        // because it is a security-relevant boundary
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.emptySet());
        Assert.assertTrue(isIpAllowed(handler, "1.2.3.4"));
        Assert.assertTrue(isIpAllowed(handler, "192.168.99.99"));
    }

    @Test
    public void testGetInstanceReturnsSingletonIgnoresNewAllowlist() {
        // First call creates the singleton with 127.0.0.1
        IpAuthHandler first = IpAuthHandler.getInstance(
                Collections.singleton("127.0.0.1"));
        // Second call with a different set must return the same instance
        // and must NOT reinitialize or override the existing allowlist
        IpAuthHandler second = IpAuthHandler.getInstance(
                Collections.singleton("192.168.0.1"));
        Assert.assertSame(first, second);
        // Original allowlist still in effect
        Assert.assertTrue(isIpAllowed(second, "127.0.0.1"));
        // New set was ignored — 192.168.0.1 should not be allowed
        Assert.assertFalse(isIpAllowed(second, "192.168.0.1"));
    }

    private static CompletableFuture<Set<String>> resolved(
            InetAddress... addresses) {
        Set<String> result = new HashSet<>();
        for (InetAddress address : addresses) {
            result.add(address.getHostAddress());
        }
        return CompletableFuture.completedFuture(
                Collections.unmodifiableSet(result));
    }

    private static CompletableFuture<Set<String>> failed(String host) {
        CompletableFuture<Set<String>> result = new CompletableFuture<>();
        result.completeExceptionally(new UnknownHostException(host));
        return result;
    }

    private static InetAddress address(int suffix) {
        try {
            return InetAddress.getByAddress(
                    new byte[]{10, 0, 0, (byte) (suffix + 1)});
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }
}
