/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.meta;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchResponse;

/**
 * Unit tests for {@link EtcdMetaDriver}'s self-healing watch: a dropped watch
 * (onError / onCompleted) must be re-subscribed so the listener is not silently
 * lost after a transport reconnect (issue #3036). Uses a mock jetcd
 * {@link Client} via the package-private test constructor; no live etcd needed.
 */
public class EtcdMetaDriverTest {

    @Test
    public void testListenReSubscribesOnError() {
        Watch watch = Mockito.mock(Watch.class);
        EtcdMetaDriver driver = newDriver(watch);

        driver.listen("k", response -> { });
        captureListener(watch).onError(new RuntimeException("watch dropped"));

        // The watch terminated, so the driver must re-subscribe: a second
        // watch() call lands once the (0ms) backoff task runs.
        Mockito.verify(watch, Mockito.timeout(2000).times(2))
               .watch(Mockito.any(ByteSequence.class),
                      Mockito.any(WatchOption.class),
                      Mockito.any(Watch.Listener.class));
    }

    @Test
    public void testListenReSubscribesOnCompleted() {
        Watch watch = Mockito.mock(Watch.class);
        EtcdMetaDriver driver = newDriver(watch);

        driver.listen("k", response -> { });
        captureListener(watch).onCompleted();

        Mockito.verify(watch, Mockito.timeout(2000).times(2))
               .watch(Mockito.any(ByteSequence.class),
                      Mockito.any(WatchOption.class),
                      Mockito.any(Watch.Listener.class));
    }

    @Test
    public void testListenPrefixReSubscribesOnError() {
        Watch watch = Mockito.mock(Watch.class);
        EtcdMetaDriver driver = newDriver(watch);

        driver.listenPrefix("prefix", response -> { });
        captureListener(watch).onError(new RuntimeException("watch dropped"));

        Mockito.verify(watch, Mockito.timeout(2000).times(2))
               .watch(Mockito.any(ByteSequence.class),
                      Mockito.any(WatchOption.class),
                      Mockito.any(Watch.Listener.class));
    }

    @Test
    public void testListenDeliversEventsToConsumer() {
        Watch watch = Mockito.mock(Watch.class);
        AtomicReference<WatchResponse> received = new AtomicReference<>();
        EtcdMetaDriver driver = newDriver(watch);

        driver.listen("k", received::set);
        WatchResponse response = Mockito.mock(WatchResponse.class);
        captureListener(watch).onNext(response);

        Assert.assertSame(response, received.get());
    }

    private static EtcdMetaDriver newDriver(Watch watch) {
        Client client = Mockito.mock(Client.class);
        Mockito.when(client.getWatchClient()).thenReturn(watch);
        EtcdMetaDriver driver = new EtcdMetaDriver(client);
        // No backoff in tests so the re-subscribe runs promptly.
        driver.reWatchDelayMs = 0L;
        return driver;
    }

    private static Watch.Listener captureListener(Watch watch) {
        ArgumentCaptor<Watch.Listener> captor =
                ArgumentCaptor.forClass(Watch.Listener.class);
        Mockito.verify(watch).watch(Mockito.any(ByteSequence.class),
                                    Mockito.any(WatchOption.class),
                                    captor.capture());
        return captor.getValue();
    }
}
