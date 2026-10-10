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

package org.apache.hugegraph.backend.store;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hugegraph.backend.Shard;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.NumericUtil;
import org.apache.hugegraph.util.StringEncoding;

import com.google.common.collect.ImmutableList;

public abstract class BackendTable<Session extends BackendSession, Entry> {

    private final String table;

    private final MetaDispatcher<Session> dispatcher;

    public BackendTable(String table) {
        this.table = table.toLowerCase();
        this.dispatcher = new MetaDispatcher<>();

        this.registerMetaHandlers();
    }

    public String table() {
        return this.table;
    }

    public MetaDispatcher<Session> metaDispatcher() {
        return this.dispatcher;
    }

    public void registerMetaHandler(String name, MetaHandler<Session> handler) {
        this.dispatcher.registerMetaHandler(name, handler);
    }

    protected void registerMetaHandlers() {
        // pass
    }

    public void updateIfPresent(Session session, Entry entry) {
        // TODO: use fine-grained row lock
        synchronized (this.table) {
            assert session == null || !session.hasChanges();
            if (this.queryExist(session, entry)) {
                this.insert(session, entry);
                if (session != null) {
                    session.commit();
                }
            }
        }
    }

    public void updateIfAbsent(Session session, Entry entry) {
        // TODO: use fine-grained row lock
        synchronized (this.table) {
            assert session == null || !session.hasChanges();
            if (!this.queryExist(session, entry)) {
                this.insert(session, entry);
                if (session != null) {
                    session.commit();
                }
            }
        }
    }

    /**
     * Mapping query-type to table-type
     *
     * @param query origin query
     * @return corresponding table type
     */
    public static HugeType tableType(Query query) {
        HugeType type = query.resultType();

        // Mapping EDGE to EDGE_OUT/EDGE_IN
        if (type == HugeType.EDGE) {
            // We assume query OUT edges
            type = HugeType.EDGE_OUT;

            while (!(query instanceof ConditionQuery ||
                     query.originQuery() == null)) {
                /*
                 * Some backends(like RocksDB) may trans ConditionQuery to
                 * IdQuery or IdPrefixQuery, so we should get the origin query.
                 */
                query = query.originQuery();
            }

            if (query.conditionsSize() > 0 && query instanceof ConditionQuery) {
                ConditionQuery cq = (ConditionQuery) query;
                // Does query IN edges
                if (cq.condition(HugeKeys.DIRECTION) == Directions.IN) {
                    type = HugeType.EDGE_IN;
                }
            }
        }

        return type;
    }

    public static final String joinTableName(String prefix, String table) {
        return prefix + "_" + table.toLowerCase();
    }

    public abstract void init(Session session);

    public abstract void clear(Session session);

    public abstract Iterator<BackendEntry> query(Session session, Query query);

    public Iterator<BackendEntry> queryOlap(Session session, Query query) {
        throw new NotImplementedException();
    }

    public abstract Number queryNumber(Session session, Query query);

    public abstract boolean queryExist(Session session, Entry entry);

    public abstract void insert(Session session, Entry entry);

    public abstract void delete(Session session, Entry entry);

    public abstract void append(Session session, Entry entry);

    public abstract void eliminate(Session session, Entry entry);

    /****************************** ShardSplitter ******************************/

    public abstract static class ShardSplitter<Session extends BackendSession> {

        // The min shard size should >= 1M to prevent too many number of shards
        protected static final int MIN_SHARD_SIZE = (int) Bytes.MB;

        // We assume the size of each key-value is 100 bytes
        protected static final int ESTIMATE_BYTES_PER_KV = 100;

        public static final String START = "";
        public static final String END = "";

        private static final byte[] EMPTY = new byte[0];
        public static final byte[] START_BYTES = new byte[]{0x0};
        public static final byte[] END_BYTES = new byte[]{-1, -1, -1, -1,
                                                          -1, -1, -1, -1,
                                                          -1, -1, -1, -1,
                                                          -1, -1, -1, -1};

        private final String table;

        public ShardSplitter(String table) {
            this.table = table;
        }

        public String table() {
            return this.table;
        }

        public List<Shard> getSplits(Session session, long splitSize) {
            E.checkArgument(splitSize >= MIN_SHARD_SIZE,
                            "The split-size must be >= %s bytes, but got %s",
                            MIN_SHARD_SIZE, splitSize);

            long size = this.estimateDataSize(session);
            if (size <= 0) {
                size = this.estimateNumKeys(session) * ESTIMATE_BYTES_PER_KV;
            }

            double count = Math.ceil(size / (double) splitSize);
            if (count <= 0) {
                count = 1;
            }
            long maxKey = this.maxKey();
            Double each = maxKey / count;

            List<Shard> splits = new ArrayList<>((int) count);
            String last = START;
            long offset = 0L;
            while (offset < maxKey) {
                offset += each.longValue();
                if (offset > maxKey) {
                    splits.add(new Shard(last, END, 0L));
                    break;
                }
                String current = this.position(offset);
                splits.add(new Shard(last, current, 0L));
                last = current;
            }
            return splits;
        }

        public final String position(long position) {
            return String.valueOf(position);
        }

        public byte[] position(String position) {
            if (END.equals(position)) {
                return null;
            }
            int value = Long.valueOf(position).intValue();
            return NumericUtil.intToBytes(value);
        }

        protected long maxKey() {
            return BytesBuffer.UINT32_MAX;
        }

        protected abstract long estimateDataSize(Session session);

        protected abstract long estimateNumKeys(Session session);

        public static class Range {

            private final byte[] startKey;
            private final byte[] endKey;

            public Range(byte[] startKey, byte[] endKey) {
                this.startKey = Arrays.equals(EMPTY, startKey) ? START_BYTES : startKey;
                this.endKey = Arrays.equals(EMPTY, endKey) ? END_BYTES : endKey;
            }

            public List<Shard> splitEven(int count) {
                if (count <= 1) {
                    return ImmutableList.of(new Shard(startKey(this.startKey),
                                                      endKey(this.endKey), 0));
                }

                byte[] start;
                byte[] end;
                boolean startChanged = false;
                boolean endChanged = false;
                int length;
                if (this.startKey.length < this.endKey.length) {
                    length = this.endKey.length;
                    start = new byte[length];
                    System.arraycopy(this.startKey, 0, start, 0,
                                     this.startKey.length);
                    end = this.endKey;
                    startChanged = true;
                } else if (this.startKey.length > this.endKey.length) {
                    length = this.startKey.length;
                    end = new byte[length];
                    System.arraycopy(this.endKey, 0, end, 0,
                                     this.endKey.length);
                    start = this.startKey;
                    endChanged = true;
                } else {
                    assert this.startKey.length == this.endKey.length;
                    length = this.startKey.length;
                    start = this.startKey;
                    end = this.endKey;
                }

                assert count > 1;
                byte[] each = align(new BigInteger(1, subtract(end, start))
                                            .divide(BigInteger.valueOf(count))
                                            .toByteArray(),
                                    length);
                byte[] offset = start;
                byte[] last = offset;
                boolean finished = false;
                List<Shard> shards = new ArrayList<>(count);
                while (Bytes.compare(offset, end) < 0 && !finished) {
                    offset = add(offset, each);
                    if (offset.length > end.length ||
                        Bytes.compare(offset, end) > 0) {
                        offset = end;
                    }
                    if (startChanged) {
                        last = this.startKey;
                        startChanged = false;
                    }
                    if (endChanged && Arrays.equals(offset, end)) {
                        offset = this.endKey;
                        finished = true;
                    }
                    shards.add(new Shard(startKey(last), endKey(offset), 0));
                    last = offset;
                }
                return shards;
            }

            private static String startKey(byte[] start) {
                return Arrays.equals(start, START_BYTES) ?
                       START : StringEncoding.encodeBase64(start);
            }

            private static String endKey(byte[] end) {
                return Arrays.equals(end, END_BYTES) ?
                       END : StringEncoding.encodeBase64(end);
            }

            private static byte[] add(byte[] array1, byte[] array2) {
                E.checkArgument(array1.length == array2.length,
                                "The length of array should be equal");
                int length = array1.length;
                byte[] result = new byte[length];
                int carry = 0;
                for (int i = length - 1; i >= 0; i--) {
                    int i1 = byte2int(array1[i]);
                    int i2 = byte2int(array2[i]);
                    int col = i1 + i2 + carry;
                    carry = (col >> 8);
                    result[i] = int2byte(col);
                }
                if (carry == 0) {
                    return result;
                }

                byte[] target = new byte[length + 1];
                target[0] = 0x1;
                System.arraycopy(result, 0, target, 1, length);
                return target;
            }

            private static byte[] subtract(byte[] array1, byte[] array2) {
                E.checkArgument(array1.length == array2.length,
                                "The length of array should be equal");
                int length = array1.length;
                byte[] result = new byte[length];
                int borrow = 0;
                for (int i = length - 1; 0 <= i; i--) {
                    int i1 = byte2int(array1[i]);
                    int i2 = byte2int(array2[i]);
                    int col = i1 - i2 + borrow;
                    borrow = (col >> 8);
                    result[i] = int2byte(col);
                }
                E.checkArgument(borrow == 0, "The array1 must >= array2");
                return result;
            }

            public static byte[] increase(byte[] array) {
                int length = array.length;
                byte[] target = new byte[length + 1];
                System.arraycopy(array, 0, target, 0, length);
                return target;
            }

            private static byte[] align(byte[] array, int length) {
                int len = array.length;
                E.checkArgument(len <= length,
                                "The length of array '%s' exceed align length '%s'", len, length);
                byte[] target = new byte[length];
                System.arraycopy(array, 0, target, length - len, len);
                return target;
            }

            private static int byte2int(byte b) {
                return (b & 0x000000ff);
            }

            private static byte int2byte(int i) {
                return (byte) (i & 0x000000ff);
            }
        }
    }
}
