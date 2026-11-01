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

package org.apache.hugegraph.store.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.store.client.type.HgStoreClientException;
import org.apache.hugegraph.store.client.util.HgBufferProxy;
import org.junit.Test;

/**
 * HgBufferProxy is the page buffer behind KvPageScanner: receive() asks the producer task for
 * more data when the buffer runs low, and close()/setError() end the stream.
 */
public class HgBufferProxyTest {

    @SuppressWarnings("unchecked")
    private static HgBufferProxy<String> proxy(Runnable task) {
        return HgBufferProxy.of(task);
    }

    @Test
    public void testRejectsNullTaskAndNullItems() {
        assertThrows(IllegalArgumentException.class, () -> HgBufferProxy.of(null));
        HgBufferProxy<String> buffer = proxy(() -> { });
        assertThrows(IllegalArgumentException.class, () -> buffer.send(null));
    }

    @Test
    public void testReceiveRunsTheProducerOnlyWhenTheBufferIsLow() {
        AtomicInteger runs = new AtomicInteger();
        AtomicReference<HgBufferProxy<String>> ref = new AtomicReference<>();
        HgBufferProxy<String> buffer = proxy(() -> ref.get().send("page-" + runs.incrementAndGet()));
        ref.set(buffer);

        assertEquals("page-1", buffer.receive(1, null));
        assertEquals(1, runs.get());

        buffer.send("x");
        buffer.send("y");
        assertEquals("x", buffer.receive(1, null));
        assertEquals("a buffered backlog must not trigger the producer", 1, runs.get());
        assertEquals("y", buffer.receive(1, null));
        assertEquals(2, runs.get());
        assertEquals("page-2", buffer.receive(1, null));
    }

    @Test
    public void testTimeoutThrowsWithoutCallbackAndNotifiesTheCallback() {
        HgBufferProxy<String> buffer = proxy(() -> { });

        RuntimeException e = assertThrows(RuntimeException.class, () -> buffer.receive(0, null));
        assertEquals("timeout, max time: 0 seconds", e.getMessage());

        List<Integer> timeouts = new ArrayList<>();
        assertNull(buffer.receive(0, timeouts::add));
        assertEquals(List.of(0), timeouts);
    }

    @Test
    public void testCloseDrainsBufferedItemsThenSignalsEnd() {
        AtomicInteger runs = new AtomicInteger();
        HgBufferProxy<String> buffer = proxy(runs::incrementAndGet);
        buffer.send("a");
        buffer.send("b");

        buffer.close();
        buffer.close();
        buffer.send("ignored");

        assertTrue(buffer.isClosed());
        assertEquals("a", buffer.receive(0, null));
        assertEquals("b", buffer.receive(0, null));
        assertNull(buffer.receive(0, null));
        assertNull(buffer.receive(0, null));
        assertEquals("a closed buffer must not ask the producer for more", 0, runs.get());
    }

    @Test
    public void testStreamErrorIsRethrownWithItsCause() {
        IllegalStateException cause = new IllegalStateException("stream broken");

        AtomicReference<HgBufferProxy<String>> ref = new AtomicReference<>();
        HgBufferProxy<String> open = proxy(() -> {
            ref.get().send("partial");
            ref.get().setError(cause);
        });
        ref.set(open);
        HgStoreClientException e = assertThrows(HgStoreClientException.class,
                                                () -> open.receive(1, null));
        assertSame(cause, e.getCause());

        HgBufferProxy<String> closed = proxy(() -> { });
        closed.send("buffered");
        closed.setError(cause);
        closed.close();
        e = assertThrows(HgStoreClientException.class, () -> closed.receive(0, null));
        assertSame(cause, e.getCause());
    }
}
