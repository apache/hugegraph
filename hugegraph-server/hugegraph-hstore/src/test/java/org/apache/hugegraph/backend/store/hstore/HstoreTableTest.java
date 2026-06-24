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
import org.apache.hugegraph.type.HugeType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class HstoreTableTest {

    @Test
    public void testRangeIndexPageStateComesFromBackendIteratorPosition() {
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
    public void testQueryByRangeUsesLegacyScanForRangeIndexQueries() {
        HstoreTable table = new HstoreTable("graph", "index");
        HstoreSessions.Session session = Mockito.mock(
                HstoreSessions.Session.class);

        IdRangeQuery query = rangeIndexQuery();
        table.queryByRange(session, query);
        verifyLegacyRangeScan(session);

        Mockito.reset(session);
        query = rangeIndexQuery();
        query.limit(10L);
        table.queryByRange(session, query);
        verifyLegacyRangeScan(session);

        Mockito.reset(session);
        query = rangeIndexQuery();
        query.offset(1L);
        table.queryByRange(session, query);
        verifyLegacyRangeScan(session);

        Mockito.reset(session);
        byte[] pagePosition = keyBytes(3);
        query = rangeIndexQuery();
        query.page(new PageState(pagePosition, 0, 1).toString());
        query.limit(10L);
        table.queryByRange(session, query);
        verifyLegacyRangeScan(session, pagePosition);
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

    private static void verifyLegacyRangeScan(HstoreSessions.Session session) {
        verifyLegacyRangeScan(session, null);
    }

    private static void verifyLegacyRangeScan(HstoreSessions.Session session,
                                              byte[] position) {
        Mockito.verify(session).scan(Mockito.anyString(),
                                     Mockito.any(byte[].class),
                                     Mockito.any(byte[].class),
                                     Mockito.any(byte[].class),
                                     Mockito.any(byte[].class),
                                     Mockito.anyInt(),
                                     Mockito.<byte[]>isNull(),
                                     positionMatcher(position));
    }

    private static byte[] positionMatcher(byte[] position) {
        if (position == null) {
            return Mockito.isNull();
        }
        return Mockito.argThat(value -> Arrays.equals(position, value));
    }
}
