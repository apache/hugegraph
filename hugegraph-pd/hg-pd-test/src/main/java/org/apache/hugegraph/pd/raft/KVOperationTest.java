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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.caucho.hessian.io.Hessian2Output;

/**
 * Pins the wire format that a follower replays from the raft log: the op byte followed by
 * Hessian-encoded key, value and arg, decoded through the class whitelist.
 */
public class KVOperationTest {

    private static final byte[] KEY = {1, 2, 3};
    private static final byte[] VALUE = {4, 5, 6, 7};

    private static KVOperation roundTrip(KVOperation op) throws IOException {
        byte[] bytes = op.toByteArray();
        Assert.assertEquals(op.getOp(), bytes[0]);
        return KVOperation.fromByteArray(bytes);
    }

    @Test
    public void testPutRoundTrip() throws IOException {
        KVOperation decoded = roundTrip(KVOperation.createPut(KEY, VALUE));

        Assert.assertEquals(KVOperation.PUT, decoded.getOp());
        Assert.assertArrayEquals(KEY, decoded.getKey());
        Assert.assertArrayEquals(VALUE, decoded.getValue());
        Assert.assertNull(decoded.getArg());
    }

    @Test
    public void testGetRoundTripKeepsEmptyValue() throws IOException {
        KVOperation decoded = roundTrip(KVOperation.createGet(KEY));

        Assert.assertEquals(KVOperation.GET, decoded.getOp());
        Assert.assertArrayEquals(KEY, decoded.getKey());
        Assert.assertArrayEquals(new byte[0], decoded.getValue());
    }

    @Test
    public void testRemoveOperationsCarryKeyAsValue() throws IOException {
        KVOperation[] ops = {
                KVOperation.createRemove(KEY),
                KVOperation.createRemoveByPrefix(KEY),
                KVOperation.createRemoveWithTTL(KEY)
        };
        byte[] expectedOps = {KVOperation.REMOVE, KVOperation.REMOVE_BY_PREFIX,
                              KVOperation.REMOVE_WITH_TTL};
        for (int i = 0; i < ops.length; i++) {
            KVOperation decoded = roundTrip(ops[i]);
            Assert.assertEquals(expectedOps[i], decoded.getOp());
            Assert.assertArrayEquals(KEY, decoded.getKey());
            Assert.assertArrayEquals(KEY, decoded.getValue());
        }
    }

    @Test
    public void testPutWithTtlRoundTripKeepsLongArg() throws IOException {
        long ttl = Integer.MAX_VALUE + 10L;
        KVOperation decoded = roundTrip(KVOperation.createPutWithTTL(KEY, VALUE, ttl));

        Assert.assertEquals(KVOperation.PUT_WITH_TTL, decoded.getOp());
        Assert.assertArrayEquals(VALUE, decoded.getValue());
        // RaftKVStore.invoke unboxes the arg with a (long) cast, so it must stay a Long
        Assert.assertEquals(Long.valueOf(ttl), decoded.getArg());
    }

    @Test
    public void testPutWithSmallTtlStillDecodesAsLong() throws IOException {
        KVOperation decoded = roundTrip(KVOperation.createPutWithTTL(KEY, VALUE, 1L));

        Assert.assertTrue(decoded.getArg() instanceof Long);
        Assert.assertEquals(1L, (long) decoded.getArg());
    }

    @Test
    public void testPutWithTtlUnitRoundTripKeepsTtlAndUnit() throws IOException {
        KVOperation decoded = roundTrip(KVOperation.createPutWithTTL(KEY, VALUE, 30L,
                                                                     TimeUnit.SECONDS));

        Assert.assertEquals(KVOperation.PUT_WITH_TTL_UNIT, decoded.getOp());
        Object[] arg = (Object[]) decoded.getArg();
        Assert.assertEquals(2, arg.length);
        Assert.assertEquals(30L, (long) arg[0]);
        Assert.assertSame(TimeUnit.SECONDS, arg[1]);
    }

    @Test
    public void testClearRoundTripHasNullKeyAndValue() throws IOException {
        KVOperation decoded = roundTrip(KVOperation.createClear());

        Assert.assertEquals(KVOperation.CLEAR, decoded.getOp());
        Assert.assertNull(decoded.getKey());
        Assert.assertNull(decoded.getValue());
    }

    @Test
    public void testSnapshotPathTravelsOnlyInAttach() throws IOException {
        KVOperation save = KVOperation.createSaveSnapshot("/data/snapshot");
        Assert.assertEquals("/data/snapshot", save.getAttach());

        // Snapshot operations are created locally by the state machine and never replicated:
        // the path is held in the transient attach field and is not part of the encoding.
        KVOperation decoded = roundTrip(save);
        Assert.assertEquals(KVOperation.SAVE_SNAPSHOT, decoded.getOp());
        Assert.assertNull(decoded.getAttach());
        Assert.assertNull(decoded.getArg());

        Assert.assertEquals(KVOperation.LOAD_SNAPSHOT,
                            KVOperation.createLoadSnapshot("/data/snapshot").getOp());
    }

    @Test
    public void testFactoriesRejectNullArguments() {
        assertNpe(() -> KVOperation.createPut(null, VALUE));
        assertNpe(() -> KVOperation.createPut(KEY, null));
        assertNpe(() -> KVOperation.createGet(null));
        assertNpe(() -> KVOperation.createPutWithTTL(null, VALUE, 1L));
        assertNpe(() -> KVOperation.createPutWithTTL(KEY, null, 1L, TimeUnit.SECONDS));
        assertNpe(() -> KVOperation.createRemove(null));
        assertNpe(() -> KVOperation.createRemoveByPrefix(null));
        assertNpe(() -> KVOperation.createRemoveWithTTL(null));
        assertNpe(() -> KVOperation.createSaveSnapshot(null));
        assertNpe(() -> KVOperation.createLoadSnapshot(null));
    }

    @Test
    public void testTruncatedEntryFailsToDecode() throws IOException {
        byte[] bytes = KVOperation.createPut(KEY, VALUE).toByteArray();
        byte[] truncated = Arrays.copyOf(bytes, bytes.length - 2);

        Assert.assertThrows(IOException.class, () -> KVOperation.fromByteArray(truncated));
    }

    @Test
    public void testDecodeNeverInstantiatesClassOutsideWhitelist() throws IOException {
        byte[] bytes = encode(KVOperation.PUT, KEY, VALUE, new NotWhitelisted("payload"));

        KVOperation decoded = KVOperation.fromByteArray(bytes);

        // The whitelist must keep the class from being instantiated
        Assert.assertFalse(decoded.getArg() instanceof NotWhitelisted);
        Assert.assertArrayEquals(KEY, decoded.getKey());
    }

    @Test
    public void testDecodeAllowsWhitelistedCollections() throws IOException {
        byte[] bytes = encode(KVOperation.PUT, KEY, VALUE,
                              new ArrayList<>(Arrays.asList("a", "b")));

        KVOperation decoded = KVOperation.fromByteArray(bytes);
        Assert.assertEquals(Arrays.asList("a", "b"), decoded.getArg());
    }

    private static byte[] encode(byte op, byte[] key, byte[] value, Object arg)
            throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(op);
        Hessian2Output output = new Hessian2Output(bos);
        output.writeObject(key);
        output.writeObject(value);
        output.writeObject(arg);
        output.flush();
        return bos.toByteArray();
    }

    private static void assertNpe(Runnable action) {
        Assert.assertThrows(NullPointerException.class, action::run);
    }

    public static class NotWhitelisted implements Serializable {

        private static final long serialVersionUID = 1L;
        public String data;

        public NotWhitelisted(String data) {
            this.data = data;
        }
    }
}
