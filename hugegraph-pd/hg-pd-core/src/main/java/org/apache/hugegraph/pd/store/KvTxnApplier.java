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

package org.apache.hugegraph.pd.store;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.kv.TxnCompare;
import org.apache.hugegraph.pd.grpc.kv.TxnEntry;
import org.apache.hugegraph.pd.grpc.kv.TxnOp;
import org.apache.hugegraph.pd.grpc.kv.TxnRecord;
import org.apache.hugegraph.pd.grpc.kv.TxnRequest;
import org.apache.hugegraph.pd.grpc.kv.TxnResponse;
import org.apache.hugegraph.pd.grpc.kv.V;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import lombok.extern.slf4j.Slf4j;

/**
 * Applies a KV TXN to a local store. Every raft node runs it on the apply thread with the
 * entry's log index, so it must be deterministic: it reads only the store and the entry,
 * never the clock (TTL expiry is ignored), and a failed compare or a rejected record is a
 * result, not an exception.
 * <p>
 * The node that proposes a TXN turns the request into its {@link TxnEntry} with
 * {@link #entry(TxnRequest)}: every key encoded as {@link KvService#put(String, String)}
 * encodes keys on that node, so KvService get and scan read what a TXN writes, and every node
 * writes the bytes the proposer chose, whatever its own default charset. Values are wrapped
 * as put wraps them.
 * <p>
 * Replay: jraft applies the entries after the last snapshot again on restart, and without a
 * snapshot that is over the store that holds them. {@link #LAST_INDEX_KEY} is written with
 * every TXN and holds its index, and RaftKVStore skips every store write (TXN or plain) at or
 * below it: all of them were applied before it. Skipping only the TXNs would not do, since a
 * plain put replayed before a skipped TXN, such as a schema write from before the upgrade,
 * would overwrite what the TXN wrote. Plain writes after the last TXN are blind writes and
 * replay in order to the same store. A store that runs without raft has no log index and
 * counts its TXNs under {@link #LOCAL_INDEX_KEY} instead, so that a store later run with raft
 * does not take that count for a raft index.
 * <p>
 * A TXN is not deduplicated: a TXN sent again applies again, and a second apply is not always
 * harmless (a guarded CREATE fails its record compare the second time). So KvClient sends a
 * TXN once, and a leader whose apply failed answers with the error instead of forwarding it.
 */
@Slf4j
public final class KvTxnApplier {

    public static final String LIVE = "LIVE";
    public static final String DROPPED = "DROPPED";
    // The origin mark of a record a CREATE wrote, "src":"create", see GraphRecord
    public static final String CREATED = "create";

    /**
     * Raw store key outside the K@ space of KvService, so no KV call reads or writes it: the
     * raft index of the last TXN applied through raft
     */
    public static final byte[] LAST_INDEX_KEY =
            "__TXN_LAST_INDEX".getBytes(StandardCharsets.UTF_8);

    /**
     * The TXN count of a store that runs without raft (raft.enable=false), which stands in
     * for the raft index there; never read on the raft path
     */
    public static final byte[] LOCAL_INDEX_KEY =
            "__TXN_LOCAL_INDEX".getBytes(StandardCharsets.UTF_8);

    private static final Gson GSON = new Gson();

    /**
     * Stands for a stored record that can't be read
     */
    private static final GraphRecord INVALID = new GraphRecord(0L, 0L, null, null);

    private static volatile TxnApplyListener listener;

    private KvTxnApplier() {
    }

    /**
     * The registration point of schema sync dispatch; null (the default) calls nothing
     */
    public static void setListener(TxnApplyListener txnListener) {
        listener = txnListener;
    }

    public static TxnApplyListener listener() {
        return listener;
    }

    /**
     * Rejects what the apply could not handle, before the TXN is proposed
     */
    public static void validate(TxnRequest request) throws PDException {
        for (TxnCompare compare : request.getComparesList()) {
            if (compare.getKey().isEmpty()) {
                throw invalid("a TXN compare needs a key");
            }
            // UNSPECIFIED is the zero value: a compare whose kind was never set
            if (compare.getKind() == TxnCompare.Kind.UNSPECIFIED ||
                compare.getKind() == TxnCompare.Kind.UNRECOGNIZED) {
                throw invalid("a TXN compare needs a kind, ABSENT or EQUALS, on key " +
                              compare.getKey());
            }
        }
        for (TxnOp op : request.getOpsList()) {
            // An empty key is the whole KV space to a DELETE_PREFIX
            if (op.getKey().isEmpty()) {
                throw invalid("a TXN op needs a key");
            }
            if (op.getType() == TxnOp.Type.UNSPECIFIED ||
                op.getType() == TxnOp.Type.UNRECOGNIZED) {
                throw invalid("a TXN op needs a type, PUT, DELETE or DELETE_PREFIX, on key " +
                              op.getKey());
            }
        }
        if (!request.hasRecord()) {
            return;
        }
        TxnRecord record = request.getRecord();
        if (record.getKey().isEmpty()) {
            throw invalid("a TXN record needs a key");
        }
        if (record.getOp() == TxnRecord.Op.UNSPECIFIED ||
            record.getOp() == TxnRecord.Op.UNRECOGNIZED) {
            throw invalid("a TXN record needs an op, BUMP, CREATE or DROP, on key " +
                          record.getKey());
        }
        if (record.getOp() == TxnRecord.Op.DROP && record.getExpectedIncarnation() <= 0L) {
            throw invalid("DROP needs the expected incarnation");
        }
    }

    /**
     * The raft index of the last TXN applied through raft, 0 before the first
     */
    public static long lastIndex(HgKVStore store) throws PDException {
        return lastIndex(store, LAST_INDEX_KEY);
    }

    /**
     * The index stored under the key: "index", or "index,term" on the raft path
     */
    public static long lastIndex(HgKVStore store, byte[] indexKey) throws PDException {
        String value = indexValue(store, indexKey);
        if (value == null) {
            return 0L;
        }
        int comma = value.indexOf(',');
        return Long.parseLong(comma < 0 ? value : value.substring(0, comma));
    }

    /**
     * The raft term of the last TXN applied through raft, 0 when none is stored (before the
     * first TXN, or an index written without its term)
     */
    public static long lastTerm(HgKVStore store) throws PDException {
        String value = indexValue(store, LAST_INDEX_KEY);
        int comma = value == null ? -1 : value.indexOf(',');
        return comma < 0 ? 0L : Long.parseLong(value.substring(comma + 1));
    }

    private static String indexValue(HgKVStore store, byte[] indexKey) throws PDException {
        byte[] bytes = store.get(indexKey);
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * The entry of a request, with its keys encoded on this node: built once, by the node
     * that proposes the TXN, or by the store without raft that applies it
     */
    public static TxnEntry entry(TxnRequest request) {
        TxnEntry.Builder entry = TxnEntry.newBuilder().setRequest(request)
                                         .setCharset(Charset.defaultCharset().name());
        for (TxnCompare compare : request.getComparesList()) {
            entry.addKeys(ByteString.copyFrom(KvService.getKeyBytes(compare.getKey())));
        }
        for (TxnOp op : request.getOpsList()) {
            entry.addKeys(ByteString.copyFrom(KvService.getKeyBytes(op.getKey())));
        }
        if (request.hasRecord()) {
            entry.addKeys(ByteString.copyFrom(KvService.getKeyBytes(request.getRecord().getKey())));
        }
        return entry.build();
    }

    /**
     * Applies an entry of the raft log at its index and term, with the registered listener
     */
    public static TxnResponse apply(HgKVStore store, TxnEntry entry, long index, long term)
            throws PDException {
        return apply(store, entry, index, term, LAST_INDEX_KEY, listener);
    }

    /**
     * @param indexKey the key that holds the index of the last applied TXN:
     *                 {@link #LAST_INDEX_KEY} on the raft path, {@link #LOCAL_INDEX_KEY} on a
     *                 store without raft
     */
    public static TxnResponse apply(HgKVStore store, TxnEntry entry, long index,
                                    byte[] indexKey, TxnApplyListener txnListener)
            throws PDException {
        return apply(store, entry, index, 0L, indexKey, txnListener);
    }

    /**
     * @param term the raft term of the entry, stored with its index so that a replay can be
     *             checked against the log it comes from; 0 stores the index alone (the path
     *             without raft)
     */
    public static TxnResponse apply(HgKVStore store, TxnEntry entry, long index, long term,
                                    byte[] indexKey, TxnApplyListener txnListener)
            throws PDException {
        if (lastIndex(store, indexKey) >= index) {
            // The store holds this entry already, and no caller waits on a replay
            return TxnResponse.getDefaultInstance();
        }
        TxnRequest request = entry.getRequest();
        int keys = request.getComparesCount() + request.getOpsCount() +
                   (request.hasRecord() ? 1 : 0);
        if (entry.getKeysCount() != keys) {
            throw invalid("a TXN entry needs one store key per compare, op and record, has " +
                          entry.getKeysCount() + " for " + keys);
        }
        // Written whatever the outcome, so that a replay does not run the TXN again
        List<KV> batch = new ArrayList<>();
        String stored = term > 0L ? index + "," + term : Long.toString(index);
        batch.add(new KV(indexKey, stored.getBytes(StandardCharsets.UTF_8)));
        TxnResponse response = evaluate(store, entry, index, batch);
        store.writeBatch(batch);
        if (response.getSucceeded() && request.hasRecord()) {
            // The listener takes rev, inc and state; the origin mark is not passed on
            notify(txnListener, index, request.getRecord().getKey(), new GraphRecord(
                    response.getRevision(), response.getIncarnation(), response.getState(),
                    null));
        }
        return response;
    }

    /**
     * Runs the compares and the record check, and adds the writes of a TXN that passes them
     * to the batch
     */
    private static TxnResponse evaluate(HgKVStore store, TxnEntry entry, long index,
                                        List<KV> batch) throws PDException {
        TxnRequest request = entry.getRequest();
        TxnResponse.Builder response = TxnResponse.newBuilder();
        int next = 0;
        for (int i = 0; i < request.getComparesCount(); i++) {
            if (!matches(store, request.getCompares(i), entry.getKeys(next++).toByteArray())) {
                return response.setFailure(TxnResponse.Failure.COMPARE_FAILED)
                               .setFailedCompare(i).build();
            }
        }

        byte[] recordKey = null;
        GraphRecord record = null;
        if (request.hasRecord()) {
            recordKey = entry.getKeys(entry.getKeysCount() - 1).toByteArray();
            GraphRecord stored = readRecord(store.get(recordKey));
            TxnResponse.Failure failure = stored == INVALID ?
                                          TxnResponse.Failure.INVALID_RECORD :
                                          check(request.getRecord(), stored);
            if (failure != TxnResponse.Failure.NONE) {
                return response.setFailure(failure)
                               .setIncarnation(stored == null ? 0L : stored.inc)
                               .setState(stored == null || stored.state == null ?
                                         "" : stored.state).build();
            }
            record = next(request.getRecord().getOp(), stored, index);
        }

        int first = batch.size();
        for (TxnOp op : request.getOpsList()) {
            byte[] key = entry.getKeys(next++).toByteArray();
            switch (op.getType()) {
                case PUT:
                    dropTtlMarker(key, store.get(key), entry.getCharset(), batch);
                    batch.add(new KV(key, wrap(op.getValue())));
                    break;
                case DELETE:
                    dropTtlMarker(key, store.get(key), entry.getCharset(), batch);
                    batch.add(new KV(key, null));
                    break;
                case DELETE_PREFIX:
                    // Earlier ops of this TXN are not in the store yet
                    batch.subList(first, batch.size())
                         .removeIf(kv -> Bytes.indexOf(kv.getKey(), key) == 0);
                    for (KV kv : store.scanPrefix(key)) {
                        dropTtlMarker(kv.getKey(), kv.getValue(), entry.getCharset(), batch);
                        batch.add(new KV(kv.getKey(), null));
                    }
                    break;
                default:
                    throw new AssertionError("Unknown TXN op " + op.getType());
            }
        }
        if (record != null) {
            batch.add(new KV(recordKey, wrap(GSON.toJson(record))));
            response.setIncarnation(record.inc).setState(record.state);
        }
        return response.setSucceeded(true).setRevision(record != null ? record.rev : index)
                       .build();
    }

    /**
     * A value put with a TTL has a marker next to it, and the TTL cleanup is meant to delete
     * the key the marker names once its time has passed, whatever the key holds by then
     * (apache/hugegraph#3378). A TXN that replaces or deletes such a value deletes the marker
     * in the same batch, so the cleanup cannot delete what the TXN wrote. Reads only the
     * stored bytes, so every node agrees.
     */
    private static void dropTtlMarker(byte[] key, byte[] stored, String charset,
                                      List<KV> batch) {
        if (stored == null) {
            return;
        }
        long expireAt;
        try {
            expireAt = V.parseFrom(stored).getTtl();
        } catch (Exception e) {
            // Not a V: a raw key, which has no marker
            return;
        }
        if (expireAt != 0L) {
            // Outside the try: a charset this node cannot encode must fail the apply here,
            // not leave this node without the delete the others make
            batch.add(new KV(KvService.ttlMarkerKey(key, expireAt, charset), null));
        }
    }

    private static TxnResponse.Failure check(TxnRecord request, GraphRecord stored) {
        long expected = request.getExpectedIncarnation();
        switch (request.getOp()) {
            case CREATE:
                return TxnResponse.Failure.NONE;
            case BUMP:
                if (stored != null && DROPPED.equals(stored.state)) {
                    return TxnResponse.Failure.GRAPH_DROPPED;
                }
                return expected != 0L && (stored == null || stored.inc != expected) ?
                       TxnResponse.Failure.INCARNATION_MISMATCH : TxnResponse.Failure.NONE;
            case DROP:
                if (stored == null || stored.inc != expected) {
                    return TxnResponse.Failure.INCARNATION_MISMATCH;
                }
                return DROPPED.equals(stored.state) ? TxnResponse.Failure.GRAPH_DROPPED :
                       TxnResponse.Failure.NONE;
            default:
                throw new AssertionError("Unknown TXN record op " + request.getOp());
        }
    }

    private static GraphRecord next(TxnRecord.Op op, GraphRecord stored, long index) {
        switch (op) {
            case BUMP:
                // An absent record is a graph created before records existed: incarnation 1
                return stored == null ? new GraphRecord(index, 1L, LIVE, null) :
                       new GraphRecord(rev(stored, index), stored.inc, stored.state, stored.src);
            case CREATE:
                return new GraphRecord(stored == null ? index : rev(stored, index),
                                       stored == null ? 1L : stored.inc + 1L, LIVE, CREATED);
            case DROP:
                return new GraphRecord(rev(stored, index), stored.inc, DROPPED, stored.src);
            default:
                throw new AssertionError("Unknown TXN record op " + op);
        }
    }

    /**
     * The next rev of a record: the index, which rises with every entry, or the stored rev
     * plus one when that is higher, as it is after raft.enable changed on the store (the
     * local count starts at 1, a new raft log too), so a rev never goes back
     */
    private static long rev(GraphRecord stored, long index) {
        return Math.max(index, stored.rev + 1L);
    }

    private static void notify(TxnApplyListener txnListener, long index, String key,
                               GraphRecord record) {
        if (txnListener == null) {
            return;
        }
        try {
            txnListener.onTxnApplied(index, key, record.rev, record.inc, record.state);
        } catch (Throwable e) {
            log.error("TXN apply listener failed at index {} for {}", index, key, e);
        }
    }

    /**
     * The stored record, null if it is absent, or {@link #INVALID} if it can't be read: not a
     * JSON object, inc or rev not a positive integer, or state not LIVE or DROPPED, as the
     * schema sync tracker and the Server read it. Treating it as absent would restart the incarnation at 1, and
     * throwing would fail the apply on every node, so the TXN fails with INVALID_RECORD
     * instead; the check reads only the bytes, so every node reaches the same result.
     */
    private static GraphRecord readRecord(byte[] stored) {
        // An empty value is stored as zero bytes: only a missing key is absent
        if (stored == null) {
            return null;
        }
        String value = null;
        try {
            value = V.parseFrom(stored).getValue();
            JsonObject json = JsonParser.parseString(value).getAsJsonObject();
            JsonPrimitive inc = json.getAsJsonPrimitive("inc");
            JsonPrimitive state = json.getAsJsonPrimitive("state");
            JsonPrimitive rev = json.getAsJsonPrimitive("rev");
            // Not validated: anything but the mark of a CREATE is no mark
            JsonElement src = json.get("src");
            String origin = src != null && src.isJsonPrimitive() &&
                            CREATED.equals(src.getAsString()) ? CREATED : null;
            if (inc.isNumber() && state.isString() && rev != null && rev.isNumber()) {
                long incarnation = inc.getAsBigDecimal().longValueExact();
                long revision = rev.getAsBigDecimal().longValueExact();
                String graphState = state.getAsString();
                if (incarnation > 0L && revision > 0L &&
                    (LIVE.equals(graphState) || DROPPED.equals(graphState))) {
                    return new GraphRecord(revision, incarnation, graphState, origin);
                }
            }
        } catch (Exception e) {
            // Not JSON, not an object, a field missing or not a primitive, or not a long
        }
        log.warn("The graph record '{}' can't be read", value);
        return INVALID;
    }

    private static boolean matches(HgKVStore store, TxnCompare compare, byte[] key)
            throws PDException {
        byte[] stored = store.get(key);
        // An empty value is stored as zero bytes: only a missing key is absent
        boolean absent = stored == null;
        if (compare.getKind() == TxnCompare.Kind.ABSENT) {
            return absent;
        }
        try {
            return !absent && V.parseFrom(stored).getValue().equals(compare.getValue());
        } catch (Exception e) {
            return false;
        }
    }

    private static byte[] wrap(String value) {
        return V.newBuilder().setValue(value).setTtl(0).build().toByteArray();
    }

    /**
     * A request that is not proposed: TXN_NOT_PROPOSED tells the caller nothing was applied,
     * where UNKNOWN is an unknown outcome
     */
    private static PDException invalid(String message) {
        return new PDException(Pdpb.ErrorType.TXN_NOT_PROPOSED_VALUE, message);
    }

    /**
     * The value of a graph record, serialized as {"rev":n,"inc":n,"state":"LIVE"}, and with
     * "src":"create" when a CREATE wrote it. The mark never changes: BUMP and DROP keep it, and
     * a record a BUMP created (a graph from before records, or one opened from a local config)
     * has none. The Server drops a LIVE record without a graph config only when it has the
     * mark, as a create that died before its config write leaves it.
     */
    private static final class GraphRecord {

        private final long rev;
        private final long inc;
        private final String state;
        // Null when absent, so it is not serialized
        private final String src;

        private GraphRecord(long rev, long inc, String state, String src) {
            this.rev = rev;
            this.inc = inc;
            this.state = state;
            this.src = src;
        }
    }
}
