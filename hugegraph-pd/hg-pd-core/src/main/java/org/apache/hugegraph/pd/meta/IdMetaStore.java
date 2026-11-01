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

package org.apache.hugegraph.pd.meta;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.store.KV;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;

import lombok.extern.slf4j.Slf4j;

/**
 * Implementation class for auto-increment ID.
 */
@Slf4j
public class IdMetaStore extends MetadataRocksDBStore {

    private static final String ID_PREFIX = "@ID@";
    private static final String CID_PREFIX = "@CID@";
    private static final String CID_SLOT_PREFIX = "@CID_SLOT@";
    private static final String CID_DEL_SLOT_PREFIX = "@CID_DEL_SLOT@";
    private static final String SEPARATOR = "@";
    private static final ConcurrentHashMap<String, Object> SEQUENCES = new ConcurrentHashMap<>();
    public static long CID_DEL_TIMEOUT = 24 * 3600 * 1000;
    // An allocation whose write was rejected for its term starts again this often at most
    private static final int ALLOCATION_ATTEMPTS = 3;
    private final long clusterId;

    public IdMetaStore(PDConfig pdConfig) {
        super(pdConfig);
        this.clusterId = pdConfig.getClusterId();
    }

    public static long bytesToLong(byte[] b) {
        ByteBuffer buf = ByteBuffer.wrap(b);
        return buf.getLong();
    }

    public static byte[] longToBytes(long l) {
        ByteBuffer buf = ByteBuffer.wrap(new byte[Long.SIZE]);
        buf.putLong(l);
        buf.flip();
        return buf.array();
    }

    /**
     * Get auto-increment ID.
     *
     * @param key
     * @param delta
     * @return
     * @throws PDException
     */
    public long getId(String key, int delta) throws PDException {
        return inLeaderTerm(term -> allocateId(key, delta, term));
    }

    private long allocateId(String key, int delta, long term) throws PDException {
        Object probableLock = getLock(key);
        byte[] keyBs = (ID_PREFIX + key).getBytes(Charset.defaultCharset());
        synchronized (probableLock) {
            byte[] bs = getOne(keyBs);
            long current = bs != null ? bytesToLong(bs) : 0L;
            long next = current + delta;
            getStore().put(keyBs, longToBytes(next), term);
            return current;
        }
    }

    /**
     * Runs an allocation that reads local state and then writes through raft, as two steps
     * and not one raft entry. The reads follow a ReadIndex wait: a leader elected a moment
     * ago may not have applied its predecessor's last write yet, and would hand out the same
     * id again. The writes carry the leader term of that wait: a request that pauses between
     * its read and its write could otherwise write its stale result after this node lost the
     * leadership and won it back, over what the leader in between allocated. jraft rejects
     * such a write, and the allocation starts again from a fresh read.
     */
    private long inLeaderTerm(Allocation allocation) throws PDException {
        for (int attempt = 1; ; attempt++) {
            // Fails with NOT_LEADER when this node is not the leader, which ends the loop
            long term = getStore().waitReadIndexInTerm();
            try {
                return allocation.run(term);
            } catch (PDException e) {
                if (e.getErrorCode() != Pdpb.ErrorType.NOT_LEADER_VALUE ||
                    attempt >= ALLOCATION_ATTEMPTS) {
                    throw e;
                }
            }
        }
    }

    private interface Allocation {

        long run(long term) throws PDException;
    }

    private Object getLock(String key) {
        Object probableLock = new Object();
        Object currentLock = SEQUENCES.putIfAbsent(key, probableLock);
        if (currentLock != null) {
            probableLock = currentLock;
        }
        return probableLock;
    }

    public void resetId(String key) throws PDException {
        Object probableLock = new Object();
        Object currentLock = SEQUENCES.putIfAbsent(key, probableLock);
        if (currentLock != null) {
            probableLock = currentLock;
        }
        byte[] keyBs = (ID_PREFIX + key).getBytes(Charset.defaultCharset());
        synchronized (probableLock) {
            removeByPrefix(keyBs);
        }
    }

    /**
     * Within 24 hours of deleting the cid identified by the name,
     * repeat applying for the same name's cid to keep the same value.
     * This design is to prevent inconsistent caching, causing data errors.
     *
     * @param key
     * @param name cid identifier
     * @param max
     * @return
     * @throws PDException
     */
    public long getCId(String key, String name, long max) throws PDException {
        // The delayed-deletion slots are read locally like the counter in getId()
        return inLeaderTerm(term -> allocateCId(key, name, max, term));
    }

    private long allocateCId(String key, String name, long max, long term)
            throws PDException {
        // Check for expired cids to delete. The frequency of deleting graphs is relatively low,
        // so this has little performance impact.
        byte[] delKeyPrefix = new StringBuffer()
                .append(CID_DEL_SLOT_PREFIX)
                .append(key).append(SEPARATOR)
                .toString().getBytes(Charset.defaultCharset());
        synchronized (this) {
            scanPrefix(delKeyPrefix).forEach(kv -> {
                long[] value = (long[]) deserialize(kv.getValue());
                if (value.length >= 2) {
                    if (System.currentTimeMillis() - value[1] > CID_DEL_TIMEOUT) {
                        try {
                            getStore().remove(genCIDSlotKey(key, value[0]), term);
                            getStore().remove(kv.getKey(), term);
                        } catch (Exception e) {
                            log.error("Exception ", e);
                        }
                    }
                }
            });

            // Restore key from delayed deletion queue
            byte[] cidDelayKey = getCIDDelayKey(key, name);
            byte[] value = getOne(cidDelayKey);
            if (value != null) {
                // Remove from delayed deletion queue
                getStore().remove(cidDelayKey, term);
                return ((long[]) deserialize(value))[0];
            } else {
                // In the term of the wait above, which covers the counter and slots too
                return allocateCId(key, max, term);
            }
        }
    }

    /**
     * Add to the deletion queue for delayed deletion.
     */
    public long delCIdDelay(String key, String name, long cid) throws PDException {
        byte[] delKey = getCIDDelayKey(key, name);
        put(delKey, serialize(new long[]{cid, System.currentTimeMillis()}));
        return cid;
    }

    /**
     * Get an auto-incrementing cyclic non-repeating ID. When the upper limit is reached, it
     * starts from 0 again.
     *
     * @param key
     * @param max the upper limit of the ID. After reaching this value, it starts incrementing
     *            from 0 again.
     * @return
     * @throws PDException
     */
    public long getCId(String key, long max) throws PDException {
        // Same two steps as getId(): read the counter and slots, then write through raft
        return inLeaderTerm(term -> allocateCId(key, max, term));
    }

    private long allocateCId(String key, long max, long term) throws PDException {
        Object probableLock = getLock(key);
        byte[] keyBs = (CID_PREFIX + key).getBytes(Charset.defaultCharset());
        synchronized (probableLock) {
            byte[] bs = getOne(keyBs);
            long current = bs != null ? bytesToLong(bs) : 0L;
            long last = current == 0 ? max - 1 : current - 1;
            {   // Find an unused cid
                List<KV> kvs = scanRange(genCIDSlotKey(key, current), genCIDSlotKey(key, max));
                for (KV kv : kvs) {
                    if (current == bytesToLong(kv.getValue())) {
                        current++;
                    } else {
                        break;
                    }
                }
            }
            if (current == max) {
                current = 0;
                List<KV> kvs = scanRange(genCIDSlotKey(key, current), genCIDSlotKey(key, last));
                for (KV kv : kvs) {
                    if (current == bytesToLong(kv.getValue())) {
                        current++;
                    } else {
                        break;
                    }
                }
            }
            if (current == last) {
                return -1;
            }
            getStore().put(genCIDSlotKey(key, current), longToBytes(current), term);
            getStore().put(keyBs, longToBytes(current + 1), term);
            return current;
        }
    }

    private byte[] genCIDSlotKey(String key, long value) {
        byte[] keySlot = (CID_SLOT_PREFIX + key + SEPARATOR).getBytes(Charset.defaultCharset());
        ByteBuffer buf = ByteBuffer.allocate(keySlot.length + Long.SIZE);
        buf.put(keySlot);
        buf.put(longToBytes(value));
        return buf.array();
    }

    private byte[] getCIDDelayKey(String key, String name) {
        byte[] bsKey = new StringBuffer()
                .append(CID_DEL_SLOT_PREFIX)
                .append(key).append(SEPARATOR)
                .append(name)
                .toString().getBytes(Charset.defaultCharset());
        return bsKey;
    }

    /**
     * Delete a cyclic ID and release its value.
     *
     * @param key
     * @param cid
     * @return
     * @throws PDException
     */
    public long delCId(String key, long cid) throws PDException {
        return remove(genCIDSlotKey(key, cid));
    }

    private byte[] serialize(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            Hessian2Output output = new Hessian2Output(bos);
            output.writeObject(obj);
            output.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Object deserialize(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            Hessian2Input input = new Hessian2Input(bis);
            Object obj = input.readObject();
            input.close();
            return obj;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
