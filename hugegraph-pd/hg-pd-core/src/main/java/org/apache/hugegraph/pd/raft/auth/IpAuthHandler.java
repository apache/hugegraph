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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class IpAuthHandler extends ChannelDuplexHandler {

    private static final long DNS_QUERY_TIMEOUT_MILLIS = 500L;
    private static final long DNS_STALE_MILLIS = 30_000L;
    private static final long DNS_REFRESH_MILLIS = 1_000L;
    private static final int MAX_CONCURRENT_DNS_QUERIES = 8;
    private static final int MAX_ALLOWED_ENTRIES = 127;
    private static final int MAX_HOST_LENGTH = 253;
    private static final int MAX_PEER_LIST_LENGTH =
            MAX_ALLOWED_ENTRIES * (MAX_HOST_LENGTH + 16);

    private final HostResolver resolver;
    private final long queryTimeoutMillis;
    private final long staleMillis;
    private final long refreshMillis;
    private final Map<String, ResolvedEntry> resolvedByEntry;
    private final Map<String, Query> inFlight;
    private final Set<String> failedEntries;
    private final ScheduledExecutorService refreshExecutor;
    private boolean closed;
    private int nextResolutionIndex;
    private List<String> resolutionOrder;
    private volatile Set<String> allowedEntries;
    private volatile Set<String> resolvedIps;
    private static volatile IpAuthHandler instance;

    private IpAuthHandler(Set<String> allowedIps) {
        this(allowedIps, new NettyHostResolver(DNS_QUERY_TIMEOUT_MILLIS), true,
             DNS_QUERY_TIMEOUT_MILLIS, DNS_STALE_MILLIS,
             DNS_REFRESH_MILLIS);
    }

    IpAuthHandler(Set<String> allowedIps, HostResolver resolver,
                  boolean scheduleRefresh, long queryTimeoutMillis,
                  long staleMillis, long refreshMillis) {
        this.resolver = resolver;
        this.queryTimeoutMillis = queryTimeoutMillis;
        this.staleMillis = staleMillis;
        this.refreshMillis = refreshMillis;
        this.resolvedByEntry = new HashMap<>();
        this.inFlight = new HashMap<>();
        this.failedEntries = new HashSet<>();
        this.nextResolutionIndex = 0;
        this.resolutionOrder = Collections.emptyList();
        try {
            this.replaceAllowedEntries(allowedIps);
        } catch (RuntimeException | Error e) {
            try {
                this.resolver.close();
            } catch (RuntimeException | Error cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
        this.resolvedIps = this.allowedEntries;
        this.closed = false;
        if (scheduleRefresh) {
            this.refreshExecutor = Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "pd-raft-dns-resolver");
                thread.setDaemon(true);
                return thread;
            });
        } else {
            this.refreshExecutor = null;
        }
        try {
            this.refreshResolvedIps();
            if (this.refreshExecutor != null) {
                this.refreshExecutor.scheduleWithFixedDelay(
                        this::refreshSafely, this.refreshMillis,
                        this.refreshMillis, TimeUnit.MILLISECONDS);
            }
        } catch (RuntimeException | Error e) {
            if (this.refreshExecutor != null) {
                this.refreshExecutor.shutdownNow();
            }
            try {
                this.resolver.close();
            } catch (RuntimeException | Error cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
    }

    public static IpAuthHandler getInstance(Set<String> allowedIps) {
        validateAllowedEntries(allowedIps);
        if (instance == null) {
            synchronized (IpAuthHandler.class) {
                if (instance == null) {
                    instance = new IpAuthHandler(allowedIps);
                }
            }
        }
        return instance;
    }

    /**
     * Returns the existing singleton instance, or null if not yet initialized.
     * Should only be called after getInstance(Set) has been called during startup.
     */
    public static IpAuthHandler getInstance() {
        return instance;
    }

    public static IpAuthHandler requireActiveInstance() {
        IpAuthHandler handler = instance;
        if (handler == null || handler.isClosed()) {
            throw new IllegalStateException(
                    "Raft peer IP allowlist is not active");
        }
        return handler;
    }

    public static void refreshInstance(Set<String> newAllowedIps) {
        requireActiveInstance().refresh(newAllowedIps);
    }

    /**
     * Refreshes the resolved IP allowlist from a new set of hostnames or IPs.
     * Should be called when the Raft peer list changes via RaftEngine#changePeerList().
     * DNS is also refreshed in the background so stable peer names can safely
     * follow address changes without blocking a Netty event loop.
     */
    public synchronized void refresh(Set<String> newAllowedIps) {
        if (this.closed) {
            throw new IllegalStateException(
                    "Raft peer IP allowlist is closed");
        }
        this.replaceAllowedEntries(newAllowedIps);
        this.resolvedByEntry.keySet().retainAll(this.allowedEntries);
        this.failedEntries.retainAll(this.allowedEntries);
        this.inFlight.entrySet().removeIf(entry -> {
            if (!this.allowedEntries.contains(entry.getKey())) {
                entry.getValue().cancel();
                return true;
            }
            return false;
        });
        this.refreshResolvedIps();
        log.info("IpAuthHandler allowlist refreshed, resolved {} entries", resolvedIps.size());
    }

    private synchronized boolean isClosed() {
        return this.closed;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        String clientIp = getClientIp(ctx);
        if (!isIpAllowed(clientIp)) {
            log.warn("Blocked connection from {}", clientIp);
            ctx.close();
            return;
        }
        super.channelActive(ctx);
    }

    private static String getClientIp(ChannelHandlerContext ctx) {
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        return remoteAddress.getAddress().getHostAddress();
    }

    private boolean isIpAllowed(String ip) {
        Set<String> resolved = this.resolvedIps;
        // Empty allowlist means no restriction is configured — allow all
        return resolved.isEmpty() || resolved.contains(ip);
    }

    synchronized void refreshResolvedIps() {
        this.refreshResolvedIps(true);
    }

    synchronized void refreshResolvedIps(boolean waitForResults) {
        if (this.closed) {
            return;
        }
        Set<String> entries = this.allowedEntries;
        this.collectQueries(entries, false);
        int attempted = 0;
        while (this.inFlight.size() < MAX_CONCURRENT_DNS_QUERIES &&
               attempted < this.resolutionOrder.size()) {
            String entry = this.resolutionOrder.get(this.nextResolutionIndex);
            this.nextResolutionIndex =
                    (this.nextResolutionIndex + 1) % this.resolutionOrder.size();
            attempted++;
            if (!this.inFlight.containsKey(entry)) {
                this.inFlight.put(
                        entry, new Query(this.resolver.resolve(entry),
                                         System.nanoTime()));
            }
        }
        this.collectQueries(entries, waitForResults);

        long staleNanos = TimeUnit.MILLISECONDS.toNanos(this.staleMillis);
        long now = System.nanoTime();
        this.resolvedByEntry.entrySet().removeIf(
                entry -> now - entry.getValue().resolvedAtNanos > staleNanos);
        Set<String> resolved = new HashSet<>(entries);
        this.resolvedByEntry.values().forEach(
                entry -> resolved.addAll(entry.addresses));
        this.resolvedIps = Collections.unmodifiableSet(resolved);
    }

    private void collectQueries(Set<String> entries,
                                boolean waitForResults) {
        long deadline = System.nanoTime() +
                        TimeUnit.MILLISECONDS.toNanos(this.queryTimeoutMillis);
        for (String entry : entries) {
            Query query = this.inFlight.get(entry);
            if (query == null) {
                continue;
            }
            CompletableFuture<ResolvedQuery> future = query.future;
            try {
                ResolvedQuery result;
                if (future.isDone()) {
                    result = future.get();
                } else if (waitForResults) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0L) {
                        expireQuery(entry, query);
                        continue;
                    }
                    result = future.get(remaining, TimeUnit.NANOSECONDS);
                } else {
                    long elapsed = System.nanoTime() - query.startedAtNanos;
                    if (elapsed > TimeUnit.MILLISECONDS.toNanos(
                            this.queryTimeoutMillis)) {
                        expireQuery(entry, query);
                    }
                    continue;
                }
                if (result.completedAtNanos - query.startedAtNanos >
                    TimeUnit.MILLISECONDS.toNanos(this.queryTimeoutMillis)) {
                    expireQuery(entry, query);
                    continue;
                }
                this.resolvedByEntry.put(
                        entry, new ResolvedEntry(result.addresses,
                                                 System.nanoTime()));
                this.inFlight.remove(entry);
                if (this.failedEntries.remove(entry)) {
                    log.info("Raft peer address resolution recovered for '{}'", entry);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                markResolutionFailure(entry, e);
                throw new IllegalStateException(
                        "Raft peer address refresh interrupted", e);
            } catch (ExecutionException e) {
                this.inFlight.remove(entry);
                markResolutionFailure(entry, e);
            } catch (TimeoutException e) {
                expireQuery(entry, query);
            } catch (CancellationException e) {
                this.inFlight.remove(entry);
                markResolutionFailure(entry, e);
            }
        }
    }

    private void expireQuery(String entry, Query query) {
        query.cancel();
        this.inFlight.remove(entry);
        markResolutionFailure(
                entry, new TimeoutException("DNS refresh deadline"));
    }

    private void markResolutionFailure(String entry, Exception failure) {
        if (this.failedEntries.add(entry)) {
            log.warn("Could not resolve Raft peer allowlist entry '{}': {}",
                     entry, failure.getMessage());
        }
    }

    private void refreshSafely() {
        try {
            this.refreshResolvedIps(false);
        } catch (RuntimeException e) {
            log.error("Unexpected Raft peer allowlist refresh failure", e);
        }
    }

    private void replaceAllowedEntries(Set<String> entries) {
        validateAllowedEntries(entries);
        Set<String> copy = new HashSet<>(entries);
        if (copy.equals(this.allowedEntries)) {
            return;
        }
        this.allowedEntries = Collections.unmodifiableSet(copy);
        this.resolutionOrder = new ArrayList<>(copy);
        Collections.sort(this.resolutionOrder);
        this.nextResolutionIndex = 0;
    }

    public static void validateAllowedEntries(Set<String> entries) {
        if (entries.size() > MAX_ALLOWED_ENTRIES) {
            throw new IllegalArgumentException(
                    "Raft peer allowlist exceeds " + MAX_ALLOWED_ENTRIES +
                    " entries");
        }
        for (String entry : entries) {
            if (entry == null || entry.isEmpty() ||
                entry.length() > MAX_HOST_LENGTH) {
                throw new IllegalArgumentException(
                        "Invalid Raft peer allowlist entry");
            }
        }
    }

    public static void validatePeerListShape(String peerList) {
        if (peerList == null || peerList.isEmpty() ||
            peerList.length() > MAX_PEER_LIST_LENGTH) {
            throw new IllegalArgumentException(
                    "Invalid Raft peer list length");
        }
        int entries = 1;
        for (int i = 0; i < peerList.length(); i++) {
            if (peerList.charAt(i) == ',' &&
                ++entries > MAX_ALLOWED_ENTRIES) {
                throw new IllegalArgumentException(
                        "Raft peer list exceeds " + MAX_ALLOWED_ENTRIES +
                        " entries");
            }
        }
    }

    synchronized void shutdown() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        if (this.refreshExecutor != null) {
            this.refreshExecutor.shutdownNow();
        }
        this.inFlight.values().forEach(Query::cancel);
        this.inFlight.clear();
        this.resolver.close();
    }

    public static synchronized void shutdownInstance() {
        if (instance != null) {
            instance.shutdown();
            instance = null;
        }
    }

    @FunctionalInterface
    interface HostResolver extends AutoCloseable {

        CompletableFuture<Set<String>> resolve(String host);

        @Override
        default void close() {
            // Most injected resolvers do not own resources.
        }
    }

    private static final class ResolvedEntry {

        private final Set<String> addresses;
        private final long resolvedAtNanos;

        private ResolvedEntry(Set<String> addresses,
                              long resolvedAtNanos) {
            this.addresses = addresses;
            this.resolvedAtNanos = resolvedAtNanos;
        }
    }

    private static final class Query {

        private final CompletableFuture<Set<String>> source;
        private final CompletableFuture<ResolvedQuery> future;
        private final long startedAtNanos;

        private Query(CompletableFuture<Set<String>> source,
                      long startedAtNanos) {
            this.source = source;
            this.startedAtNanos = startedAtNanos;
            this.future = source.thenApply(
                    addresses -> new ResolvedQuery(addresses,
                                                   System.nanoTime()));
        }

        private void cancel() {
            this.source.cancel(true);
            this.future.cancel(true);
        }
    }

    private static final class ResolvedQuery {

        private final Set<String> addresses;
        private final long completedAtNanos;

        private ResolvedQuery(Set<String> addresses,
                              long completedAtNanos) {
            this.addresses = addresses;
            this.completedAtNanos = completedAtNanos;
        }
    }

    private static final class NettyHostResolver implements HostResolver {

        private final NioEventLoopGroup eventLoopGroup;
        private final DnsNameResolver resolver;

        private NettyHostResolver(long queryTimeoutMillis) {
            this.eventLoopGroup = new NioEventLoopGroup(1, task -> {
                Thread thread = new Thread(task, "pd-raft-dns-event-loop");
                thread.setDaemon(true);
                return thread;
            });
            try {
                this.resolver = new DnsNameResolverBuilder(
                        this.eventLoopGroup.next())
                        .channelType(NioDatagramChannel.class)
                        .ttl(0, 1)
                        .negativeTtl(0)
                        .queryTimeoutMillis(queryTimeoutMillis)
                        .build();
            } catch (RuntimeException | Error e) {
                this.eventLoopGroup.shutdownGracefully(
                        0L, DNS_QUERY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                                   .awaitUninterruptibly(
                                           DNS_QUERY_TIMEOUT_MILLIS);
                throw e;
            }
        }

        @Override
        public CompletableFuture<Set<String>> resolve(String host) {
            io.netty.util.concurrent.Future<List<InetAddress>> query =
                    this.resolver.resolveAll(host);
            CompletableFuture<Set<String>> result = new CompletableFuture<>();
            query.addListener(done -> {
                if (!done.isSuccess()) {
                    result.completeExceptionally(done.cause());
                    return;
                }
                Set<String> addresses = new HashSet<>();
                for (InetAddress address : query.getNow()) {
                    addresses.add(address.getHostAddress());
                }
                result.complete(Collections.unmodifiableSet(addresses));
            });
            result.whenComplete((ignored, failure) -> {
                if (result.isCancelled()) {
                    query.cancel(true);
                }
            });
            return result;
        }

        @Override
        public void close() {
            this.resolver.close();
            this.eventLoopGroup.shutdownGracefully(
                    0L, DNS_QUERY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                                   .awaitUninterruptibly(
                                           DNS_QUERY_TIMEOUT_MILLIS);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        String clientIp = getClientIp(ctx);
        log.warn("Client : {} connection exception : {}", clientIp, cause);
        if (ctx.channel().isActive()) {
            ctx.close().addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("Client: {} connection closed failed: {}",
                             clientIp, future.cause().getMessage());
                }
            });
        }
    }
}
