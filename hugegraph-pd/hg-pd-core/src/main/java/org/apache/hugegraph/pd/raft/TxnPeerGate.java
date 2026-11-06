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

package org.apache.hugegraph.pd.raft;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;

import lombok.extern.slf4j.Slf4j;

/**
 * Keeps a TXN from being proposed while a peer of the raft group may not apply its opcode. A
 * PD from before the TXN entry logs an unknown op and goes on, so a leader that proposed a
 * TXN to such a peer would commit an entry the peer never applies, and the peer would serve
 * stale metadata once it leads. Every peer of the current configuration (voters and learners,
 * not this node) must have answered the member probe with a {@code maxKvOp} at or above
 * {@link KVOperation#TXN} once. That answer is kept in this process and recorded through raft
 * with the endpoint of the leader that recorded it ({@link Record}), so a new leader after a
 * failover trusts what an earlier leader confirmed and probes only the peers outside that set
 * (a release cannot go down, and a PD downgrade after the first TXN is unsupported). A peer
 * that has not been confirmed, or that answered with a lower opcode, blocks every TXN with
 * {@code TXN_NOT_PROPOSED} naming it; a confirmed peer that is unreachable later does not
 * block, on this leader or the next. Unconfirmed peers are probed again on a TXN attempt at
 * most once per {@code probeIntervalMs}, and at once when the configuration changed, so a
 * peer added to the group starts unconfirmed.
 * <p>
 * A membership change goes through {@link #confirm(Collection)} first: every peer it adds is
 * probed at once and an unconfirmed or older one refuses the change. A peer that joins later
 * catches up by applying the log, TXN entries included, so it must apply them; checking it at
 * join time also closes the window between a TXN's check and its proposal. Peers the change
 * keeps are covered by {@link #check()} before every TXN, so they are not probed here.
 */
@Slf4j
public class TxnPeerGate {

    /**
     * Where confirmations outlive this leader: the set every node of the group can read, and
     * a best-effort write of a larger one. A failed or late write only makes the next leader
     * probe more.
     */
    public interface Record {

        Record NONE = new Record() {
            @Override
            public Set<String> confirmed() {
                return Set.of();
            }

            @Override
            public void record(Set<String> confirmed) {
            }
        };

        Set<String> confirmed();

        void record(Set<String> confirmed);
    }

    private final Supplier<Collection<String>> peers;
    private final Supplier<Record> record;
    private final Function<String, CompletableFuture<RaftRpcProcessor.GetMemberResponse>> probe;
    private final long probeIntervalMs;
    private final long probeTimeoutMs;
    private final LongSupplier clock;

    private final Set<String> confirmed = ConcurrentHashMap.newKeySet();
    // Why an unconfirmed peer is not confirmed, from its last probe
    private final Map<String, String> reasons = new ConcurrentHashMap<>();
    private Set<String> lastConfiguration = Set.of();
    private long lastProbeAt = Long.MIN_VALUE;

    /**
     * @param peers           the endpoints of the current configuration, without this node
     * @param probe           the member probe of a peer, by endpoint
     * @param probeIntervalMs how long an unconfirmed peer is left alone after a probe
     * @param probeTimeoutMs  how long a probe is waited for
     * @param clock           milliseconds, monotonic
     */
    public TxnPeerGate(Supplier<Collection<String>> peers,
                       Function<String, CompletableFuture<RaftRpcProcessor.GetMemberResponse>>
                               probe,
                       long probeIntervalMs, long probeTimeoutMs, LongSupplier clock) {
        this(peers, probe, probeIntervalMs, probeTimeoutMs, clock, () -> Record.NONE);
    }

    /**
     * @param record where confirmations are recorded through raft, looked up on each use
     */
    public TxnPeerGate(Supplier<Collection<String>> peers,
                       Function<String, CompletableFuture<RaftRpcProcessor.GetMemberResponse>>
                               probe,
                       long probeIntervalMs, long probeTimeoutMs, LongSupplier clock,
                       Supplier<Record> record) {
        this.record = record;
        this.peers = peers;
        this.probe = probe;
        this.probeIntervalMs = probeIntervalMs;
        this.probeTimeoutMs = probeTimeoutMs;
        this.clock = clock;
    }

    /**
     * Returns when every peer of the configuration is confirmed
     *
     * @throws PDException TXN_NOT_PROPOSED, naming a peer that is not
     */
    public synchronized void check() throws PDException {
        Set<String> configuration = new LinkedHashSet<>(this.peers.get());
        List<String> unconfirmed = unconfirmed(configuration);
        if (unconfirmed.isEmpty()) {
            return;
        }
        long now = this.clock.getAsLong();
        boolean changed = !configuration.equals(this.lastConfiguration);
        if (changed || now - this.lastProbeAt >= this.probeIntervalMs) {
            this.lastConfiguration = configuration;
            this.lastProbeAt = now;
            probe(unconfirmed);
            unconfirmed = unconfirmed(configuration);
        }
        if (!unconfirmed.isEmpty()) {
            throw notConfirmed("txn not proposed", unconfirmed, configuration.size());
        }
    }

    /**
     * Probes, at once, every peer a membership change is about to add that is not confirmed
     * yet, and returns when all of them are
     *
     * @param endpoints the peers the new configuration adds, without this node
     * @throws PDException TXN_NOT_PROPOSED, naming a peer that is not confirmed
     */
    public synchronized void confirm(Collection<String> endpoints) throws PDException {
        Set<String> configuration = new LinkedHashSet<>(endpoints);
        List<String> unconfirmed = unconfirmed(configuration);
        if (unconfirmed.isEmpty()) {
            return;
        }
        probe(unconfirmed);
        unconfirmed = unconfirmed(configuration);
        if (!unconfirmed.isEmpty()) {
            throw notConfirmed("membership change refused", unconfirmed, configuration.size());
        }
    }

    private PDException notConfirmed(String what, List<String> unconfirmed, int peers) {
        String peer = unconfirmed.get(0);
        return new PDException(Pdpb.ErrorType.TXN_NOT_PROPOSED_VALUE,
                               what + ": PD peer " + peer + " is not confirmed to apply the " +
                               "txn entry (" + this.reasons.getOrDefault(peer, "not probed yet") +
                               "). Every PD node must run a release with the txn RPC; " +
                               unconfirmed.size() + " of " + peers + " peers unconfirmed");
    }

    /**
     * The peers this process has confirmed, for the probes and the tests
     */
    public Set<String> confirmed() {
        return Set.copyOf(this.confirmed);
    }

    private List<String> unconfirmed(Set<String> configuration) {
        List<String> unconfirmed = new ArrayList<>();
        Set<String> recorded = recorded();
        for (String peer : configuration) {
            if (!this.confirmed.contains(peer) && !recorded.contains(peer)) {
                unconfirmed.add(peer);
            }
        }
        return unconfirmed;
    }

    private Set<String> recorded() {
        try {
            return this.record.get().confirmed();
        } catch (RuntimeException e) {
            log.warn("Reading the confirmed PD peers failed, probing as if none: {}",
                     e.getMessage());
            return Set.of();
        }
    }

    /**
     * Records every peer confirmed so far, with those an earlier leader recorded
     */
    private void recordConfirmed() {
        Set<String> all = new TreeSet<>(recorded());
        if (!all.addAll(this.confirmed)) {
            return;
        }
        try {
            this.record.get().record(all);
        } catch (RuntimeException e) {
            log.warn("Recording the confirmed PD peers {} failed: {}", all, e.getMessage());
        }
    }

    /**
     * Probes the peers at once and waits for each answer
     */
    private void probe(List<String> endpoints) {
        Map<String, CompletableFuture<RaftRpcProcessor.GetMemberResponse>> futures =
                new LinkedHashMap<>();
        for (String endpoint : endpoints) {
            try {
                futures.put(endpoint, this.probe.apply(endpoint));
            } catch (RuntimeException e) {
                futures.put(endpoint, CompletableFuture.failedFuture(e));
            }
        }
        for (Map.Entry<String, CompletableFuture<RaftRpcProcessor.GetMemberResponse>> entry :
                futures.entrySet()) {
            String peer = entry.getKey();
            try {
                RaftRpcProcessor.GetMemberResponse response =
                        entry.getValue().get(this.probeTimeoutMs, TimeUnit.MILLISECONDS);
                if (response != null && response.getMaxKvOp() >= KVOperation.TXN) {
                    this.confirmed.add(peer);
                    this.reasons.remove(peer);
                    log.info("PD peer {} applies the txn entry (max KV op {})", peer,
                             response.getMaxKvOp());
                } else {
                    String reason = "it answered with max KV op " +
                                    (response == null ? 0 : response.getMaxKvOp()) +
                                    ", a PD release without the txn RPC";
                    this.reasons.put(peer, reason);
                    log.warn("PD peer {} blocks txn: {}", peer, reason);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                this.reasons.put(peer, "the probe was interrupted");
            } catch (Exception e) {
                String reason = "it did not answer the probe: " + e.getMessage();
                this.reasons.put(peer, reason);
                log.warn("PD peer {} blocks txn: {}", peer, reason);
            }
        }
        recordConfirmed();
    }
}
