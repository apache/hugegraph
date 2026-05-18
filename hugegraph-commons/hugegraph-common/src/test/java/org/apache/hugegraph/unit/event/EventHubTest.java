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

package org.apache.hugegraph.unit.event;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.event.Event;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;

public class EventHubTest extends BaseUnitTest {

    private static final int THREADS_NUM = 8;

    private EventHub eventHub = null;

    @BeforeClass
    public static void init() {
        EventHub.init(THREADS_NUM);
    }

    @AfterClass
    public static void clear() throws InterruptedException {
        EventHub.destroy(30);
    }

    @Before
    public void setup() {
        this.eventHub = new EventHub("test");
        Assert.assertEquals("test", this.eventHub.name());
    }

    @After
    public void teardown() {
        this.eventHub = null;
    }

    private void wait100ms() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testEventGetListenerNonResult() {
        Assert.assertFalse(this.eventHub.containsListener("not-exist"));
        Assert.assertEquals(0, this.eventHub.listeners("not-exist").size());
    }

    @Test
    public void testEventAddListener() {
        final String event = "event-test";

        EventListener listener = (Event e) -> null;

        this.eventHub.listen(event, listener);

        Assert.assertTrue(this.eventHub.containsListener(event));
        Assert.assertEquals(1, this.eventHub.listeners(event).size());
        Assert.assertEquals(listener, this.eventHub.listeners(event).get(0));
    }

    @Test
    public void testEventAddListenerTwice() {
        final String event = "event-test";

        EventListener listener = (Event e) -> null;

        this.eventHub.listen(event, listener);
        this.eventHub.listen(event, listener);

        Assert.assertTrue(this.eventHub.containsListener(event));
        Assert.assertEquals(2, this.eventHub.listeners(event).size());
        Assert.assertEquals(listener, this.eventHub.listeners(event).get(0));
        Assert.assertEquals(listener, this.eventHub.listeners(event).get(1));
    }

    @Test
    public void testEventRemoveListener() {
        final String event = "event-test";

        EventListener listener = (Event e) -> null;

        this.eventHub.listen(event, listener);

        Assert.assertTrue(this.eventHub.containsListener(event));
        Assert.assertEquals(1, this.eventHub.listeners(event).size());
        Assert.assertEquals(listener, this.eventHub.listeners(event).get(0));

        Assert.assertEquals(1, this.eventHub.unlisten(event, listener));

        Assert.assertFalse(this.eventHub.containsListener(event));
        Assert.assertEquals(0, this.eventHub.listeners(event).size());
    }

    @Test
    public void testEventRemoveListenerButNonResult() {
        final String event = "event-test";

        EventListener listener = (Event e) -> null;

        this.eventHub.listen(event, listener);

        Assert.assertTrue(this.eventHub.containsListener(event));
        Assert.assertEquals(1, this.eventHub.listeners(event).size());
        Assert.assertEquals(listener, this.eventHub.listeners(event).get(0));

        Assert.assertEquals(0, this.eventHub.unlisten(event, null));
        Assert.assertEquals(0, this.eventHub.unlisten("event-fake", listener));

        Assert.assertTrue(this.eventHub.containsListener(event));
        Assert.assertEquals(1, this.eventHub.listeners(event).size());
        Assert.assertEquals(listener, this.eventHub.listeners(event).get(0));
    }

    @Test
    public void testEventRemoveListenerOfOneInTwo() {
        final String event1 = "event-test1";
        final String event2 = "event-test2";

        EventListener listener = (Event e) -> null;

        this.eventHub.listen(event1, listener);
        this.eventHub.listen(event2, listener);

        Assert.assertTrue(this.eventHub.containsListener(event1));
        Assert.assertEquals(1, this.eventHub.listeners(event1).size());
        Assert.assertEquals(listener, this.eventHub.listeners(event1).get(0));

        Assert.assertTrue(this.eventHub.containsListener(event2));
        Assert.assertEquals(1, this.eventHub.listeners(event2).size());
        Assert.assertEquals(listener, this.eventHub.listeners(event2).get(0));

        Assert.assertEquals(1, this.eventHub.unlisten(event1, listener));

        Assert.assertFalse(this.eventHub.containsListener(event1));
        Assert.assertFalse(this.eventHub.containsListener(event1));
        Assert.assertEquals(0, this.eventHub.listeners(event1).size());

        Assert.assertTrue(this.eventHub.containsListener(event2));
        Assert.assertEquals(1, this.eventHub.listeners(event2).size());
        Assert.assertEquals(listener, this.eventHub.listeners(event2).get(0));
    }

    @Test
    public void testEventRemoveListenerByEvent() {
        final String event = "event-test";

        EventListener listener1 = (Event e) -> null;
        EventListener listener2 = (Event e) -> null;

        this.eventHub.listen(event, listener1);
        this.eventHub.listen(event, listener2);

        Assert.assertTrue(this.eventHub.containsListener(event));
        Assert.assertEquals(2, this.eventHub.listeners(event).size());
        Assert.assertEquals(listener1, this.eventHub.listeners(event).get(0));
        Assert.assertEquals(listener2, this.eventHub.listeners(event).get(1));

        Assert.assertEquals(2, this.eventHub.unlisten(event).size());

        Assert.assertFalse(this.eventHub.containsListener(event));
        Assert.assertEquals(0, this.eventHub.listeners(event).size());
    }

    @Test
    public void testEventRemoveListenerByEventButNonResult() {
        final String event = "event-test";

        EventListener listener1 = (Event e) -> null;
        EventListener listener2 = (Event e) -> null;

        this.eventHub.listen(event, listener1);
        this.eventHub.listen(event, listener2);

        Assert.assertTrue(this.eventHub.containsListener(event));
        Assert.assertEquals(2, this.eventHub.listeners(event).size());
        Assert.assertEquals(listener1, this.eventHub.listeners(event).get(0));
        Assert.assertEquals(listener2, this.eventHub.listeners(event).get(1));

        Assert.assertEquals(0, this.eventHub.unlisten("event-fake").size());

        Assert.assertEquals(2, this.eventHub.listeners(event).size());
    }

    @Test
    public void testEventRemoveListenerByEventOf2SameListener() {
        final String event = "event-test";

        EventListener listener = (Event e) -> null;

        this.eventHub.listen(event, listener);
        this.eventHub.listen(event, listener);

        Assert.assertTrue(this.eventHub.containsListener(event));
        Assert.assertEquals(2, this.eventHub.listeners(event).size());
        Assert.assertEquals(listener, this.eventHub.listeners(event).get(0));

        Assert.assertEquals(2, this.eventHub.unlisten(event, listener));

        Assert.assertFalse(this.eventHub.containsListener(event));
        Assert.assertEquals(0, this.eventHub.listeners(event).size());
    }

    @Test
    public void testEventCallWithoutArg() {
        final String call = "event-call";

        this.eventHub.listen(call, event -> {
            Assert.assertEquals(call, event.name());
            Assert.assertEquals(0, event.args().length);
            return "fake-event-result";
        });

        Assert.assertEquals("fake-event-result", this.eventHub.call(call));
    }

    @Test
    public void testEventCallWithArg1() {
        final String call = "event-call";

        this.eventHub.listen(call, event -> {
            Assert.assertEquals(call, event.name());

            event.checkArgs(Float.class);

            Object[] args = event.args();
            Assert.assertEquals(1, args.length);
            Assert.assertEquals(3.14f, args[0]);

            return "fake-event-result";
        });

        Assert.assertEquals("fake-event-result",
                            this.eventHub.call(call, 3.14f));
    }

    @Test
    public void testEventCallWithArg2() {
        final String call = "event-call";

        this.eventHub.listen(call, event -> {
            Assert.assertEquals(call, event.name());

            event.checkArgs(String.class, Integer.class);

            Object[] args = event.args();
            Assert.assertEquals(2, args.length);
            Assert.assertEquals("fake-arg0", args[0]);
            Assert.assertEquals(123, args[1]);

            return "fake-event-result";
        });

        Assert.assertEquals("fake-event-result",
                            this.eventHub.call(call, "fake-arg0", 123));
    }

    @Test
    public void testEventCallWithArg2ButArgNotMatched() {
        final String call = "event-call";

        this.eventHub.listen(call, event -> {
            Assert.assertEquals(call, event.name());

            event.checkArgs(String.class, Integer.class);

            Object[] args = event.args();
            Assert.assertEquals(2, args.length);
            Assert.assertEquals("fake-arg0", args[0]);
            Assert.assertEquals(123, args[1]);

            return "fake-event-result";
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.eventHub.call(call, "fake-arg0");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.eventHub.call(call, "fake-arg0", 123, "456");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.eventHub.call(call, 123, "fake-arg0");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.eventHub.call(call, "fake-arg0", 123f);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.eventHub.call(call, "fake-arg0", "123");
        });
    }

    @Test
    public void testEventNotify() {
        final String notify = "event-notify";
        AtomicInteger count = new AtomicInteger();

        this.eventHub.listen(notify, event -> {
            Assert.assertEquals(notify, event.name());
            Assert.assertEquals(0, event.args().length);
            count.incrementAndGet();
            return true;
        });

        this.eventHub.notify(notify);

        // Maybe should improve
        this.wait100ms();

        Assert.assertEquals(1, count.get());
    }

    @Test
    public void testEventNotifyAny() {
        AtomicInteger count = new AtomicInteger();

        this.eventHub.listen(EventHub.ANY_EVENT, event -> {
            Assert.assertTrue(ImmutableList.of("event1", "event2")
                                           .contains(event.name()));
            Assert.assertEquals(0, event.args().length);
            count.incrementAndGet();
            return true;
        });

        this.eventHub.notify("event1");
        this.eventHub.notify("event2");

        // Maybe should improve
        this.wait100ms();

        Assert.assertEquals(2, count.get());
    }

    @Test
    public void testEventNotifyWithArg2() {
        final String notify = "event-notify";
        AtomicInteger count = new AtomicInteger();

        this.eventHub.listen(notify, event -> {
            Assert.assertEquals(notify, event.name());

            event.checkArgs(String.class, Integer.class);

            Object[] args = event.args();
            Assert.assertEquals("fake-arg0", args[0]);
            Assert.assertEquals(123, args[1]);

            count.incrementAndGet();
            return true;
        });

        this.eventHub.notify(notify, "fake-arg0", 123);

        // Maybe should improve
        this.wait100ms();

        Assert.assertEquals(1, count.get());
    }

    @Test
    public void testNotifyExcept() throws Exception {
        final String notify = "event-notify";
        AtomicInteger listenerACount = new AtomicInteger();
        AtomicInteger listenerBCount = new AtomicInteger();
        AtomicInteger listenerCCount = new AtomicInteger();

        EventListener listenerA = event -> {
            event.checkArgs(String.class);
            Assert.assertEquals("fake-arg", event.args()[0]);
            listenerACount.incrementAndGet();
            return true;
        };
        EventListener listenerB = event -> {
            listenerBCount.incrementAndGet();
            return true;
        };
        EventListener listenerC = event -> {
            event.checkArgs(String.class);
            Assert.assertEquals("fake-arg", event.args()[0]);
            listenerCCount.incrementAndGet();
            return true;
        };

        this.eventHub.listen(notify, listenerA);
        this.eventHub.listen(notify, listenerB);
        this.eventHub.listen(EventHub.ANY_EVENT, listenerC);

        Assert.assertEquals(2, (int) this.eventHub
                                          .notifyExcept(notify, listenerB,
                                                        "fake-arg")
                                          .get());
        Assert.assertEquals(1, listenerACount.get());
        Assert.assertEquals(0, listenerBCount.get());
        Assert.assertEquals(1, listenerCCount.get());
    }

    @Test
    public void testEventNotifyWithMultiThreads() throws InterruptedException {
        final String notify = "event-notify";

        EventListener listener1 = event -> {
            Assert.assertEquals(notify, event.name());
            event.checkArgs(Integer.class);
            return null;
        };

        EventListener listener2 = event -> {
            Assert.assertEquals(notify, event.name());

            event.checkArgs(Integer.class);
            int i = (int) event.args()[0];
            if (i % 10000 == 0) {
                System.out.println("On event '" + notify + "': " + i);
            }
            return null;
        };

        Thread listenerUpdateThread = new Thread(() -> {
            // This will cost about 1s
            for (int i = 0; i < 10; i++) {
                this.eventHub.listen(notify, listener1);
                if (!this.eventHub.listeners(notify).contains(listener2)) {
                    this.eventHub.listen(notify, listener2);
                }

                this.wait100ms();

                if (i % 2 == 0) {
                    this.eventHub.unlisten(notify);
                } else {
                    this.eventHub.unlisten(notify, listener1);
                }
            }
        });
        listenerUpdateThread.start();

        runWithThreads(THREADS_NUM, () -> {
            // This will cost about 1s ~ 2s
            for (int i = 0; i < 10000 * 10; i++) {
                this.eventHub.notify(notify, i);
                Thread.yield();
            }
        });

        listenerUpdateThread.join();
    }

    @Test
    public void testEventCallWithMultiThreads() {
        final String call = "event-call";

        EventListener listener = event -> {
            Assert.assertEquals(call, event.name());

            event.checkArgs(Integer.class);
            int i = (int) event.args()[0];
            return i;
        };

        this.eventHub.listen(call, listener);

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < 10000 * 1000; i++) {
                Assert.assertEquals(i, this.eventHub.call(call, i));
            }
        });
    }
}
