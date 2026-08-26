/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.store.hstore;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hugegraph.backend.id.Id.IdType;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.query.IdRangeQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.BackendEntryIterator;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.type.HugeType;
import org.junit.Assert;
import org.junit.Test;

public class HstoreTableTest {

    @Test
    public void testHstoreDoesNotAdvertiseInputIdOrdering() {
        Assert.assertFalse(new HstoreFeatures()
                                   .supportsQuerySortByInputIds());
    }

    @Test
    public void testRangeIndexPageStateUsesNextUnreadPhysicalKey() {
        Query query = new Query(HugeType.RANGE_INT_INDEX);
        query.page("");
        query.limit(1L);

        BackendEntryIterator iterator = HstoreTable.newEntryIterator(
                new TestColumnIterator(1, 2), query);

        Assert.assertTrue(iterator.hasNext());
        BackendEntry entry = iterator.next();
        Assert.assertArrayEquals(keyBytes(1), entry.id().asBytes());

        PageState pageState = PageInfo.pageState(iterator);
        Assert.assertArrayEquals(keyBytes(2), pageState.position());
        Assert.assertEquals(1L, pageState.total());
    }

    @Test
    public void testRangeIndexPagingUsesPagePositionAsInclusiveScanStart() {
        byte[] originalStart = keyBytes(1);
        byte[] pagePosition = keyBytes(2);
        IdRangeQuery query = rangeIndexQuery();

        query.page("");
        Assert.assertArrayEquals(originalStart,
                                 HstoreTable.rangeIndexScanStart(
                                         query, originalStart));

        query.page(new PageState(pagePosition, 0, 1).toString());
        Assert.assertArrayEquals(pagePosition,
                                 HstoreTable.rangeIndexScanStart(
                                         query, originalStart));
        int type = HstoreTable.rangeIndexScanType(
                query, HstoreSessions.Session.SCAN_GT_BEGIN |
                       HstoreSessions.Session.SCAN_LT_END);
        Assert.assertTrue(HstoreSessions.Session.matchScanType(
                HstoreSessions.Session.SCAN_GTE_BEGIN, type));
        Assert.assertTrue(HstoreSessions.Session.matchScanType(
                HstoreSessions.Session.SCAN_LT_END, type));
    }

    @Test
    public void testOrderedRangeScanIsScopedToOrderSensitiveIndexes() {
        IdRangeQuery query = rangeIndexQuery();
        Assert.assertFalse(HstoreTable.shouldUseOrderedRangeScan(query));

        query.limit(10L);
        Assert.assertTrue(HstoreTable.shouldUseOrderedRangeScan(query));

        query = rangeIndexQuery();
        query.offset(1L);
        Assert.assertTrue(HstoreTable.shouldUseOrderedRangeScan(query));

        query = rangeIndexQuery();
        query.page("");
        Assert.assertTrue(HstoreTable.shouldUseOrderedRangeScan(query));

        query = new IdRangeQuery(HugeType.VERTEX, null,
                                 IdGenerator.of(keyBytes(1), IdType.STRING),
                                 true,
                                 IdGenerator.of(keyBytes(9), IdType.STRING),
                                 false);
        query.limit(10L);
        Assert.assertFalse(HstoreTable.shouldUseOrderedRangeScan(query));
    }

    @Test
    public void testRangeScanBudgetIncludesOneLookaheadRecord() {
        IdRangeQuery query = rangeIndexQuery();
        Assert.assertEquals(HgStoreClientConst.NO_LIMIT,
                            HstoreTable.rangeScanBudget(query));

        query.limit(10L);
        Assert.assertEquals(11L, HstoreTable.rangeScanBudget(query));

        query.offset(3L);
        Assert.assertEquals(14L, HstoreTable.rangeScanBudget(query));
    }

    private static IdRangeQuery rangeIndexQuery() {
        return new IdRangeQuery(HugeType.RANGE_INT_INDEX, null,
                                IdGenerator.of(keyBytes(1), IdType.STRING),
                                true,
                                IdGenerator.of(keyBytes(9), IdType.STRING),
                                false);
    }

    private static byte[] keyBytes(int key) {
        byte[] bytes = new byte[9];
        bytes[0] = HugeType.RANGE_INT_INDEX.code();
        bytes[8] = (byte) key;
        return bytes;
    }

    private static final class TestColumnIterator
            implements BackendColumnIterator {

        private final List<Integer> keys;
        private int offset;
        private byte[] position;

        private TestColumnIterator(Integer... keys) {
            this.keys = Arrays.asList(keys);
            this.offset = 0;
            this.position = null;
        }

        @Override
        public boolean hasNext() {
            return this.offset < this.keys.size();
        }

        @Override
        public BackendColumn next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] key = keyBytes(this.keys.get(this.offset++));
            this.position = key;
            return BackendColumn.of(key, key);
        }

        @Override
        public void close() {
            // pass
        }

        @Override
        public byte[] position() {
            return this.position;
        }
    }
}
