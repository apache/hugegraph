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

package org.apache.hugegraph.pd.core;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftRpcClient;
import org.apache.hugegraph.pd.raft.RaftStateMachine;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.pd.store.HgKVStoreImpl;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.rpc.RpcServer;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PDNativeOwnerTest {

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 10000)
    public void testCloseWaitsForReadersAndReleasesNativeOwners() throws Exception {
        RocksDB.loadLibrary();
        Path path = this.temporary.newFolder().toPath();
        PDConfig config = new PDConfig();
        config.setDataPath(path.toString());
        HgKVStoreImpl store = new HgKVStoreImpl();
        store.init(config);
        RocksDB db = Whitebox.getInternalState(store, "db");
        Options options = Whitebox.getInternalState(store, "dbOptions");
        ReadWriteLock lock = Whitebox.getInternalState(store, "readWriteLock");
        store.put(new byte[]{1}, new byte[]{2});
        store.put(new byte[]{3}, new byte[]{4});
        Assert.assertEquals(1, store.scanPrefix(new byte[]{1}).size());
        Assert.assertEquals(2, store.scanRange(new byte[]{0}, new byte[]{4}).size());
        store.removeByPrefix(new byte[]{1});
        Assert.assertEquals(0, store.scanPrefix(new byte[]{1}).size());
        CountDownLatch closing = new CountDownLatch(1);
        FutureTask<Void> close = new FutureTask<>(() -> {
            closing.countDown();
            store.close();
            return null;
        });
        Thread closer = new Thread(close, "pd-owner-close");
        lock.readLock().lock();
        try {
            closer.start();
            Assert.assertTrue(closing.await(1, TimeUnit.SECONDS));
            Assert.assertThrows(TimeoutException.class, () -> close.get(50, TimeUnit.MILLISECONDS));
            Assert.assertTrue(db.isOwningHandle());
            Assert.assertTrue(options.isOwningHandle());
        } finally {
            lock.readLock().unlock();
        }
        close.get(2, TimeUnit.SECONDS);
        store.close();
        Assert.assertFalse(db.isOwningHandle());
        Assert.assertFalse(options.isOwningHandle());
        HgKVStoreImpl reopened = new HgKVStoreImpl();
        reopened.init(config);
        try {
            Assert.assertArrayEquals(new byte[]{4}, reopened.get(new byte[]{3}));
        } finally {
            reopened.close();
        }
    }

    @Test
    public void testInitializationFailureReleasesOptions() {
        RocksDB.loadLibrary();
        HgKVStoreImpl store = new HgKVStoreImpl();
        PDConfig config = mock(PDConfig.class);
        AtomicReference<Options> allocated = new AtomicReference<>();
        IllegalStateException injected = new IllegalStateException("injected configuration failure");
        when(config.getDataPath()).thenAnswer(invocation -> {
            allocated.set(Whitebox.getInternalState(store, "dbOptions"));
            throw injected;
        });
        IllegalStateException failure = Assert.assertThrows(IllegalStateException.class, () -> store.init(config));
        Assert.assertSame(injected, failure.getCause());
        Assert.assertFalse(allocated.get().isOwningHandle());
        store.close();
    }

    @Test
    public void testTerminalFactoryCloseRejectsLazyReopen() throws Exception {
        Field storeField = MetadataFactory.class.getDeclaredField("store");
        Field shutdownField = MetadataFactory.class.getDeclaredField("shutdown");
        storeField.setAccessible(true);
        shutdownField.setAccessible(true);
        synchronized (MetadataFactory.class) {
            Object previous = storeField.get(null);
            boolean shutdown = shutdownField.getBoolean(null);
            HgKVStore owned = mock(HgKVStore.class);
            try {
                storeField.set(null, owned);
                shutdownField.setBoolean(null, false);
                MetadataFactory.shutdownStore();
                MetadataFactory.shutdownStore();
                verify(owned, times(1)).close();
                Assert.assertThrows(IllegalStateException.class, () -> MetadataFactory.getStore(null));
            } finally {
                storeField.set(null, previous);
                shutdownField.setBoolean(null, shutdown);
            }
        }
    }

    @Test
    public void testInterruptedRaftJoinRetainsOwnerAndClosesRpcClient() throws Exception {
        RaftEngine engine = newEngine();
        RaftGroupService group = mock(RaftGroupService.class);
        Node node = mock(Node.class);
        RpcServer server = mock(RpcServer.class);
        RaftRpcClient client = mock(RaftRpcClient.class);
        Whitebox.setInternalState(engine, "raftGroupService", group);
        Whitebox.setInternalState(engine, "raftNode", node);
        Whitebox.setInternalState(engine, "rpcServer", server);
        Whitebox.setInternalState(engine, "raftRpcClient", client);
        AtomicInteger attempts = new AtomicInteger();
        doAnswer(invocation -> {
            Assert.assertSame(node, engine.getRaftNode());
            if (attempts.incrementAndGet() == 1) {
                throw new InterruptedException("injected join interruption");
            }
            return null;
        }).when(group).join();
        try {
            engine.shutDown();
            Assert.assertTrue(Thread.interrupted());
            Assert.assertEquals(2, attempts.get());
            Assert.assertNull(engine.getRaftNode());
            engine.shutDown();
            verify(group, times(1)).shutdown();
            verify(node, never()).shutdown();
            verify(server, times(1)).shutdown();
            verify(client, times(1)).shutdown();
        } finally {
            Thread.interrupted();
        }
    }

    @Test(timeout = 10000)
    public void testLeaderCallbacksDrainBeforeStoppingRaft() throws Exception {
        RaftEngine engine = newEngine();
        RaftStateMachine state = Whitebox.getInternalState(engine, "stateMachine");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        state.addStateListener(() -> {
            calls.incrementAndGet();
            entered.countDown();
            try {
                Assert.assertTrue(release.await(3, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        state.onLeaderStart(1);
        Assert.assertTrue(entered.await(1, TimeUnit.SECONDS));
        FutureTask<Void> stop = new FutureTask<>(() -> {
            engine.stopLeaderCallbacks();
            return null;
        });
        Thread stopper = new Thread(stop, "pd-leader-drain");
        try {
            stopper.start();
            Assert.assertThrows(TimeoutException.class, () -> stop.get(50, TimeUnit.MILLISECONDS));
            release.countDown();
            stop.get(2, TimeUnit.SECONDS);
            state.onLeaderStart(2);
            engine.stopLeaderCallbacks();
            Assert.assertEquals(1, calls.get());
        } finally {
            release.countDown();
            stopper.join(3000);
        }
    }

    private static RaftEngine newEngine() throws Exception {
        Constructor<RaftEngine> constructor = RaftEngine.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }
}
