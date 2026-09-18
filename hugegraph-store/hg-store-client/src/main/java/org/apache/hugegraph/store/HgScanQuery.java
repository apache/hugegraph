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

package org.apache.hugegraph.store;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.store.client.util.HgAssert;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;

/**
 * 2022/3/4
 *
 * @version 0.5.0
 */
public interface HgScanQuery {

    static HgScanQuery tableOf(String table) {
        return ScanBuilder.tableOf(table).build();
    }

    static HgScanQuery rangeOf(String table, List<HgOwnerKey> startList, List<HgOwnerKey> endList) {
        return ScanBuilder.rangeOf(table, startList, endList).build();
    }

    static HgScanQuery prefixOf(String table, List<HgOwnerKey> prefixList) {
        return ScanBuilder.prefixOf(table, prefixList).build();
    }

    static HgScanQuery prefixOf(String table, List<HgOwnerKey> prefixList,
                                ScanOrderType orderType) {
        return ScanBuilder.prefixOf(table, prefixList).setOrderType(orderType).build();
    }

    static HgScanQuery prefixIteratorOf(String table, Iterator<HgOwnerKey> prefixItr) {
        return ScanBuilder.prefixIteratorOf(table, prefixItr).build();
    }

    static HgScanQuery prefixIteratorOf(String table, Iterator<HgOwnerKey> prefixItr,
                                        ScanOrderType orderType) {
        return ScanBuilder.prefixIteratorOf(table, prefixItr).setOrderType(orderType).build();
    }

    String getTable();

    HgScanQuery.ScanMethod getScanMethod();

    List<HgOwnerKey> getPrefixList();

    Iterator<HgOwnerKey> getPrefixItr();

    List<HgOwnerKey> getStartList();

    List<HgOwnerKey> getEndList();

    long getLimit();

    long getPerKeyLimit();

    long getPerKeyMax();

    long getSkipDegree();

    int getScanType();

    ScanOrderType getOrderType();

    boolean isOnlyKey();

    byte[] getQuery();

    ScanBuilder builder();

    enum ScanMethod {
        ALL,
        PREFIX,
        RANGE
    }

    enum SortType {
        UNSORTED,
        SORT_BY_EDGE,
        SORT_BY_VERTEX
    }

    class ScanBuilder {

        private final String table;
        private final HgScanQuery.ScanMethod sanMethod;
        private long limit = Integer.MAX_VALUE;
        private long perKeyLimit = Integer.MAX_VALUE;
        private long perKeyMax = Integer.MAX_VALUE;
        private int scanType;
        private ScanOrderType orderType;

        private long skipDegree;

        private boolean onlyKey;
        private byte[] query;
        private List<HgOwnerKey> prefixList;
        private List<HgOwnerKey> startList;
        private List<HgOwnerKey> endList;
        private Iterator<HgOwnerKey> prefixItr;

        ScanBuilder(HgScanQuery.ScanMethod sanMethod, String table) {
            this.table = table;
            this.sanMethod = sanMethod;
            this.orderType = ScanOrderType.ORDER_NONE;
        }

        public static ScanBuilder rangeOf(String table, List<HgOwnerKey> startList,
                                          List<HgOwnerKey> endList) {
            HgAssert.isArgumentValid(table, "table");
            HgAssert.isArgumentValid(startList, "startList");
            HgAssert.isArgumentValid(endList, "endList");
            HgAssert.isTrue(startList.size() == endList.size()
                    , "The size of startList not equals endList's.");

            ScanBuilder res = new ScanBuilder(HgScanQuery.ScanMethod.RANGE, table);
            res.startList = startList;
            res.endList = endList;
            res.scanType = HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LTE_END;
            return res;
        }

        public static ScanBuilder prefixOf(String table, List<HgOwnerKey> prefixList) {
            HgAssert.isArgumentValid(table, "table");
            HgAssert.isArgumentValid(prefixList, "prefixList");

            ScanBuilder res = new ScanBuilder(HgScanQuery.ScanMethod.PREFIX, table);
            res.prefixList = prefixList;
            return res;

        }

        public static ScanBuilder prefixIteratorOf(String table, Iterator<HgOwnerKey> prefixItr) {
            HgAssert.isArgumentValid(table, "table");

            ScanBuilder res = new ScanBuilder(HgScanQuery.ScanMethod.PREFIX, table);
            res.prefixItr = prefixItr;
            return res;

        }

        public static ScanBuilder tableOf(String table) {
            HgAssert.isArgumentValid(table, "table");

            return new ScanBuilder(HgScanQuery.ScanMethod.ALL, table);
        }

        public ScanBuilder setLimit(long limit) {
            this.limit = limit;
            return this;
        }

        public ScanBuilder setPerKeyLimit(long limit) {
            this.perKeyLimit = limit;
            return this;
        }

        public ScanBuilder setPerKeyMax(long max) {
            this.perKeyMax = max;
            return this;
        }

        public ScanBuilder setScanType(int scanType) {
            this.scanType = scanType;
            return this;
        }

        public ScanBuilder setOrderType(ScanOrderType orderType) {
            HgAssert.isFalse(orderType == ScanOrderType.ORDER_BY_KEY,
                             "ORDER_BY_KEY is not supported by batch scan");
            this.orderType = orderType;
            return this;
        }

        public ScanBuilder setQuery(byte[] query) {
            this.query = query;
            return this;
        }

        public ScanBuilder setSkipDegree(long skipDegree) {
            this.skipDegree = skipDegree;
            return this;
        }

        public ScanBuilder setOnlyKey(boolean onlyKey) {
            this.onlyKey = onlyKey;
            return this;
        }

        public HgScanQuery build() {
            return this.new BatchScanQuery();
        }

        private class BatchScanQuery implements HgScanQuery {

            @Override
            public String getTable() {
                return table;
            }

            @Override
            public HgScanQuery.ScanMethod getScanMethod() {
                return sanMethod;
            }

            @Override
            public List<HgOwnerKey> getPrefixList() {
                if (prefixList == null) {
                    return Collections.EMPTY_LIST;
                } else {
                    return Collections.unmodifiableList(prefixList);
                }
            }

            @Override
            public Iterator<HgOwnerKey> getPrefixItr() {
                return prefixItr;
            }

            @Override
            public List<HgOwnerKey> getStartList() {
                if (startList == null) {
                    return Collections.EMPTY_LIST;
                } else {
                    return Collections.unmodifiableList(startList);
                }
            }

            @Override
            public List<HgOwnerKey> getEndList() {
                if (endList == null) {
                    return Collections.EMPTY_LIST;
                } else {
                    return Collections.unmodifiableList(endList);
                }
            }

            @Override
            public long getLimit() {
                return limit;
            }

            @Override
            public long getPerKeyLimit() {
                return perKeyLimit;
            }

            @Override
            public long getPerKeyMax() {
                return perKeyMax;
            }

            @Override
            public long getSkipDegree() {
                return skipDegree;
            }

            @Override
            public int getScanType() {
                return scanType;
            }

            @Override
            public ScanOrderType getOrderType() {
                return orderType;
            }

            @Override
            public boolean isOnlyKey() {
                return onlyKey;
            }

            @Override
            public byte[] getQuery() {
                return query;
            }

            @Override
            public ScanBuilder builder() {
                return ScanBuilder.this;
            }

            @Override
            public String toString() {
                final StringBuffer sb = new StringBuffer("HgScanQuery{");
                sb.append("table='").append(getTable()).append('\'');
                sb.append(", scanMethod=").append(getScanMethod());
                sb.append(", prefixList=").append(getPrefixList());
                sb.append(", startList=").append(getStartList());
                sb.append(", endList=").append(getEndList());
                sb.append(", limit=").append(getLimit());
                sb.append(", perKeyLimit=").append(getPerKeyLimit());
                sb.append(", perKeyMax=").append(getPerKeyMax());
                sb.append(", skipDegree=").append(getSkipDegree());
                sb.append(", scanType=").append(getScanType());
                sb.append(", orderType=").append(getOrderType());
                sb.append(", onlyKey=").append(isOnlyKey());
                sb.append(", query=");
                if (query == null) {
                    sb.append("null");
                } else {
                    sb.append('[');
                    for (int i = 0; i < query.length; ++i) {
                        sb.append(i == 0 ? "" : ", ").append(query[i]);
                    }
                    sb.append(']');
                }
                sb.append('}');
                return sb.toString();
            }
        }

    }
}
