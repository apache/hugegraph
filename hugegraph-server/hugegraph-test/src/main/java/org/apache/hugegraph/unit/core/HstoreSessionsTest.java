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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.hugegraph.backend.store.hstore.HstoreSessions;
import org.apache.hugegraph.backend.store.hstore.HstoreSessionsImpl;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgSessionProvider;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.mockito.Mockito;
import org.junit.Assert;
import org.junit.Test;

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
}
