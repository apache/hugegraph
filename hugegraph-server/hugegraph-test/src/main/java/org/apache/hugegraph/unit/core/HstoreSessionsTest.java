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

package org.apache.hugegraph.unit.core;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.hstore.HstoreSessions;
import org.apache.hugegraph.backend.store.hstore.HstoreStore;
import org.apache.hugegraph.backend.store.hstore.HstoreStore.HstoreGraphStore;
import org.apache.hugegraph.backend.store.hstore.HstoreTables;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.serializer.OlapKey;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.backend.store.hstore.HstoreSessionsImpl;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgSessionProvider;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class HstoreSessionsTest {

    @Test
    public void testPoolRejectsBorrowAfterLastHstoreSessionCloses() {
        Boolean initialized = Whitebox.getInternalState(HstoreSessionsImpl.class, "initializedNode");
        HgStoreClient previous = Whitebox.getInternalState(HstoreSessionsImpl.class, "hgStoreClient");
        HgStoreClient client = new HgStoreClient();
        HgSessionProvider provider = Mockito.mock(HgSessionProvider.class);
        Mockito.when(provider.createSession(Mockito.anyString(), Mockito.isNull()))
               .thenReturn(Mockito.mock(HgStoreSession.class));
        Whitebox.setInternalState(client, "sessionProvider", provider);
        Whitebox.setInternalState(HstoreSessionsImpl.class, "initializedNode", Boolean.TRUE);
        Whitebox.setInternalState(HstoreSessionsImpl.class, "hgStoreClient", client);
        try {
            HstoreSessionsImpl pool = new HstoreSessionsImpl(FakeObjects.newConfig(), "test", "closed");
            pool.getOrNewSession();
            Assert.assertTrue(pool.close());
            Assert.assertTrue(pool.closed());
            try {
                pool.getOrNewSession();
                Assert.fail("closed HStore pool accepted a new borrower");
            } catch (IllegalStateException expected) {
                Assert.assertEquals("Backend session pool is closed", expected.getMessage());
            }
            Assert.assertTrue(pool.closed());
            Mockito.verify(provider, Mockito.times(2)).createSession(Mockito.anyString(), Mockito.isNull());
        } finally {
            Whitebox.setInternalState(HstoreSessionsImpl.class, "hgStoreClient", previous);
            Whitebox.setInternalState(HstoreSessionsImpl.class, "initializedNode", initialized);
        }
    }

    @Test
    public void testOrderedScanDoesNotAddAbstractSubclassRequirement()
            throws Exception {
        Method method = HstoreSessions.Session.class.getDeclaredMethod(
                "scanOrdered", String.class, byte[].class, byte[].class,
                byte[].class, byte[].class, int.class, byte[].class,
                long.class);

        Assert.assertFalse(Modifier.isAbstract(method.getModifiers()));
    }

    @Test
    public void testClearOlapDeletesOnlyMatchingPropertyAndCommits() throws Exception {
        OlapFixture fixture = new OlapFixture();
        byte[] merged = fixture.add(5, IdGenerator.of("alice"), false);
        byte[] legacy = fixture.add(5, IdGenerator.of("legacy"), true);
        fixture.add(8, IdGenerator.of("alice"), false);
        // The legacy vertex key equals the target property prefix but belongs to property 8.
        fixture.add(8, IdGenerator.of(5), true);
        fixture.store.clearOlapTable(IdGenerator.of(5));
        Assert.assertEquals(2, fixture.rows.size());
        Mockito.verify(fixture.session).delete(fixture.table, IdGenerator.of("alice").asBytes(), merged);
        Mockito.verify(fixture.session).delete(fixture.table, IdGenerator.of("legacy").asBytes(), legacy);
        Mockito.verify(fixture.session).commit();
        Mockito.verify(fixture.columns).close();
        Mockito.verify(fixture.sessions, Mockito.never()).dropTable(Mockito.any(String[].class));
        Mockito.verify(fixture.sessions, Mockito.never()).truncateTable(Mockito.anyString());
    }

    @Test
    public void testRemoveOlapKeepsSharedTableAndOtherProperty() throws Exception {
        OlapFixture fixture = new OlapFixture();
        fixture.add(5, IdGenerator.of("alice"), false);
        fixture.add(8, IdGenerator.of("alice"), false);
        fixture.store.removeOlapTable(IdGenerator.of(5));
        Assert.assertEquals(1, fixture.rows.size());
        Mockito.verify(fixture.session).commit();
        Mockito.verify(fixture.columns).close();
        Mockito.verify(fixture.sessions, Mockito.never()).dropTable(Mockito.any(String[].class));
        Assert.assertTrue(fixture.store.existOlapTable(IdGenerator.of(8)));
    }

    @Test
    public void testOlapCleanupCommitsBoundedBatches() throws Exception {
        OlapFixture fixture = new OlapFixture();
        for (int i = 0; i < 1025; i++) {
            fixture.add(5, IdGenerator.of(i + 1), false);
        }
        fixture.store.clearOlapTable(IdGenerator.of(5));
        Assert.assertTrue(fixture.rows.isEmpty());
        Mockito.verify(fixture.session, Mockito.atLeast(2)).commit();
        Mockito.verify(fixture.columns).close();
    }

    @Test
    public void testOlapCleanupRollbackAndCloseOnCommitFailure() throws Exception {
        OlapFixture fixture = new OlapFixture();
        fixture.add(5, IdGenerator.of("alice"), false);
        RuntimeException failure = new IllegalStateException("commit refused");
        Mockito.doThrow(failure).when(fixture.session).commit();
        Assert.assertSame(failure, Assert.assertThrows(RuntimeException.class,
                () -> fixture.store.clearOlapTable(IdGenerator.of(5))));
        Assert.assertEquals(1, fixture.rows.size());
        Mockito.verify(fixture.session).rollback();
        Mockito.verify(fixture.columns).close();
    }

    @Test
    public void testOlapCleanupDoesNotCommitPendingGraphMutation() throws Exception {
        OlapFixture fixture = new OlapFixture();
        fixture.pending.add("unrelated mutation");
        Assert.assertThrows(IllegalStateException.class,
                            () -> fixture.store.clearOlapTable(IdGenerator.of(5)));
        Assert.assertEquals(1, fixture.pending.size());
        Mockito.verify(fixture.session, Mockito.never()).scan(Mockito.anyString());
        Mockito.verify(fixture.session, Mockito.never()).commit();
        Mockito.verify(fixture.session, Mockito.never()).rollback();
    }

    @Test
    public void testOlapCleanupRejectsMismatchedCompoundKey() throws Exception {
        OlapFixture fixture = new OlapFixture();
        byte[] key = fixture.add(8, IdGenerator.of("alice"), false);
        fixture.rows.get(Base64.getEncoder().encodeToString(key)).value =
                BytesBuffer.allocate(16).writeVInt(5).writeVInt(99).bytes();
        IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                () -> fixture.store.clearOlapTable(IdGenerator.of(5)));
        Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(Bytes.toHex(key)));
        Assert.assertEquals(1, fixture.rows.size());
        Mockito.verify(fixture.session).rollback();
        Mockito.verify(fixture.columns).close();
        Mockito.verify(fixture.session, Mockito.never()).delete(Mockito.anyString(),
                                                               Mockito.any(byte[].class), Mockito.any(byte[].class));
    }

    @Test
    public void testOlapCleanupRejectsMismatchedCompoundValue() throws Exception {
        assertInvalidTargetValue(BytesBuffer.allocate(16).writeVInt(8).writeVInt(99).bytes());
    }

    @Test
    public void testOlapCleanupRejectsEmptyTargetCompoundValue() throws Exception {
        assertInvalidTargetValue(new byte[0]);
    }

    @Test
    public void testOlapCleanupRejectsUnreadableTargetCompoundValue() throws Exception {
        assertInvalidTargetValue(new byte[]{(byte) 0x81});
    }

    @Test
    public void testOlapCleanupBoundsInvalidKeyDiagnostic() throws Exception {
        OlapFixture fixture = new OlapFixture();
        byte[] key = fixture.add(5, IdGenerator.of("alice".repeat(40)), false);
        fixture.rows.get(Base64.getEncoder().encodeToString(key)).value = new byte[0];
        IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                () -> fixture.store.clearOlapTable(IdGenerator.of(5)));
        String prefix = Bytes.toHex(Arrays.copyOf(key, 64));
        Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(prefix + "..."));
        Assert.assertFalse(failure.getMessage().contains(Bytes.toHex(key)));
        Assert.assertEquals(1, fixture.rows.size());
        Mockito.verify(fixture.session).rollback();
        Mockito.verify(fixture.columns).close();
    }

    @Test
    public void testOlapCleanupPreservesUnreadableOtherCompoundValue() throws Exception {
        OlapFixture fixture = new OlapFixture();
        byte[] key = fixture.add(8, IdGenerator.of("alice"), false);
        fixture.rows.get(Base64.getEncoder().encodeToString(key)).value = new byte[]{(byte) 0x81};
        fixture.store.clearOlapTable(IdGenerator.of(5));
        Assert.assertEquals(1, fixture.rows.size());
        Mockito.verify(fixture.columns).close();
        Mockito.verify(fixture.session, Mockito.never()).delete(Mockito.anyString(),
                                                               Mockito.any(byte[].class), Mockito.any(byte[].class));
        Mockito.verify(fixture.session, Mockito.never()).commit();
    }

    private static void assertInvalidTargetValue(byte[] value) throws Exception {
        OlapFixture fixture = new OlapFixture();
        byte[] key = fixture.add(5, IdGenerator.of("alice"), false);
        fixture.rows.get(Base64.getEncoder().encodeToString(key)).value = value;
        IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                () -> fixture.store.clearOlapTable(IdGenerator.of(5)));
        Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(Bytes.toHex(key)));
        Assert.assertEquals(1, fixture.rows.size());
        Mockito.verify(fixture.session).rollback();
        Mockito.verify(fixture.columns).close();
        Mockito.verify(fixture.session, Mockito.never()).delete(Mockito.anyString(),
                                                               Mockito.any(byte[].class), Mockito.any(byte[].class));
        Mockito.verify(fixture.session, Mockito.never()).commit();
    }

    @Test
    public void testOlapExistsUsesSharedPhysicalTable() throws Exception {
        OlapFixture fixture = new OlapFixture();
        Assert.assertTrue(fixture.store.existOlapTable(IdGenerator.of(5)));
        Mockito.verify(fixture.sessions).existsTable(fixture.table);
    }

    private static class OlapFixture {

        final HstoreSessions sessions = Mockito.mock(HstoreSessions.class);
        final HstoreSessions.Session session = Mockito.mock(HstoreSessions.Session.class);
        final BackendColumnIterator columns = Mockito.mock(BackendColumnIterator.class);
        final HstoreGraphStore store = new HstoreGraphStore(Mockito.mock(BackendStoreProvider.class), "test", "g");
        final String table = new HstoreTables.OlapTable("g").table();
        final Map<String, BackendColumn> rows = new LinkedHashMap<>();
        final List<String> pending = new ArrayList<>();

        OlapFixture() throws Exception {
            Field field = HstoreStore.class.getDeclaredField("sessions");
            field.setAccessible(true);
            field.set(this.store, this.sessions);
            Mockito.when(this.sessions.session()).thenReturn(this.session);
            Mockito.when(this.session.opened()).thenReturn(true);
            Mockito.when(this.sessions.existsTable(this.table)).thenReturn(true);
            Mockito.when(this.session.scan(this.table)).thenAnswer(invocation -> {
                Iterator<BackendColumn> iterator = new ArrayList<>(this.rows.values()).iterator();
                Mockito.when(this.columns.hasNext()).thenAnswer(ignored -> iterator.hasNext());
                Mockito.when(this.columns.next()).thenAnswer(ignored -> iterator.next());
                return this.columns;
            });
            Mockito.when(this.session.hasChanges()).thenAnswer(invocation -> !this.pending.isEmpty());
            Mockito.doAnswer(invocation -> {
                Assert.assertEquals(this.table, invocation.getArgument(0));
                this.pending.add(Base64.getEncoder().encodeToString(invocation.getArgument(2)));
                return null;
            }).when(this.session).delete(Mockito.anyString(), Mockito.any(byte[].class), Mockito.any(byte[].class));
            Mockito.when(this.session.commit()).thenAnswer(invocation -> {
                int count = this.pending.size();
                this.pending.forEach(this.rows::remove);
                this.pending.clear();
                return count;
            });
            Mockito.doAnswer(invocation -> {
                this.pending.clear();
                return null;
            }).when(this.session).rollback();
        }

        byte[] add(int property, Id vertex, boolean legacy) {
            byte[] key = legacy ? BytesBuffer.allocate(vertex.length() + 9).writeId(vertex).bytes() :
                         OlapKey.format(IdGenerator.of(property), vertex);
            byte[] value = BytesBuffer.allocate(16).writeVInt(property).writeVInt(99).bytes();
            this.rows.put(Base64.getEncoder().encodeToString(key), BackendColumn.of(key, value));
            return key;
        }
    }
}
