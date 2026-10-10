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

package org.apache.hugegraph.store.client.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.store.HgKvIterator;
import org.junit.Test;

public class CommonKvStreamObserverTest {

    @Test
    public void testCompletedEmptyStreamSkipsTimedPoll() throws Exception {
        RecordingQueue queue = new RecordingQueue();
        CommonKvStreamObserver<List<Integer>, Integer> observer = observer(queue);
        observer.onCompleted();
        StreamKvIterator<Integer> stream = stream(observer);
        assertFalse(stream.hasNext());
        stream.close();
        assertEquals("completed empty stream must not enter timed poll", 0, queue.emptyTimedPolls.get());
    }

    @Test
    public void testCompletedSingleStreamAndCloseSkipEmptyTimedPoll() throws Exception {
        RecordingQueue queue = new RecordingQueue();
        CommonKvStreamObserver<List<Integer>, Integer> observer = observer(queue);
        observer.onNext(Collections.singletonList(1));
        observer.onCompleted();
        StreamKvIterator<Integer> stream = stream(observer);
        assertTrue(stream.hasNext());
        assertEquals(Integer.valueOf(1), stream.next());
        assertFalse(stream.hasNext());
        assertFalse(stream.hasNext());
        stream.close();
        assertEquals("exhausted terminal stream must not enter empty timed poll", 0, queue.emptyTimedPolls.get());
    }

    @Test
    public void testCompletedThreeStreamsMergeAndCloseSkipEmptyTimedPoll() throws Exception {
        List<RecordingQueue> queues = new ArrayList<>();
        MultiStreamIterator<Integer> merged = merged(queues);
        List<Integer> actual = drain(merged);
        merged.close();
        assertEquals(Arrays.asList(1, 2, 3), actual);
        assertEquals("merged terminal streams and close must not enter empty timed poll", 0,
                     queues.stream().mapToInt(queue -> queue.emptyTimedPolls.get()).sum());
    }

    @Test(timeout = 5000)
    public void testFinalBatchPublishedAtTerminalBoundaryIsNotLost() throws Exception {
        CountDownLatch boundary = new CountDownLatch(1);
        CountDownLatch published = new CountDownLatch(1);
        GatedQueue queue = new GatedQueue(boundary, published);
        CommonKvStreamObserver<List<Integer>, Integer> observer = observer(queue);
        StreamKvIterator<Integer> stream = stream(observer);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                assertTrue("consumer must reach terminal/queue boundary", boundary.await(2, TimeUnit.SECONDS));
                observer.onNext(Collections.singletonList(7));
                observer.onCompleted();
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                published.countDown();
            }
        }, "publish-final-client-batch");
        producer.start();
        try {
            assertTrue("final batch must remain visible when terminal state changes", stream.hasNext());
            assertEquals(Integer.valueOf(7), stream.next());
            assertFalse(stream.hasNext());
            assertEquals(null, failure.get());
        } finally {
            boundary.countDown();
            published.countDown();
            producer.join(2000);
            assertFalse("producer must finish", producer.isAlive());
            stream.close();
        }
    }

    @Test(timeout = 5000)
    public void testAcceptedResponseRefreshesTimeoutBeforeParsing() throws Exception {
        CountDownLatch parsing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger polls = new AtomicInteger();
        RecordingQueue queue = new RecordingQueue() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<Integer> poll(long timeout, TimeUnit unit) throws InterruptedException {
                if (polls.getAndIncrement() == 0) {
                    // Exercise the timeout check while the accepted response is still being parsed.
                    return null;
                }
                release.countDown();
                return super.poll(timeout, unit);
            }
        };
        CommonKvStreamObserver<List<Integer>, Integer> observer = parsingObserver(parsing, release);
        observer.setTimeout(1000);
        Field queueField = CommonKvStreamObserver.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        queueField.set(observer, queue);
        Field current = CommonKvStreamObserver.class.getDeclaredField("current");
        current.setAccessible(true);
        current.setLong(observer, System.nanoTime() - TimeUnit.SECONDS.toNanos(2));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                observer.onNext(Collections.singletonList(7));
            } catch (Throwable error) {
                failure.set(error);
            }
        });
        producer.start();
        try {
            assertTrue(parsing.await(2, TimeUnit.SECONDS));
            assertEquals(Integer.valueOf(7), observer.consume().next());
            producer.join(2000);
            assertFalse(producer.isAlive());
            assertEquals(null, failure.get());
            assertEquals(null, observer.consume());
        } finally {
            release.countDown();
            producer.join(2000);
            observer.clear();
        }
    }

    @Test(timeout = 5000)
    public void testCompletionWaitsForResponseBeingParsed() throws Exception {
        CountDownLatch parsing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CommonKvStreamObserver<List<Integer>, Integer> observer = parsingObserver(parsing, release);
        Thread producer = new Thread(() -> observer.onNext(Collections.singletonList(7)));
        producer.start();
        try {
            assertTrue(parsing.await(2, TimeUnit.SECONDS));
            observer.onCompleted();
            assertFalse("completion must wait for the accepted batch", observer.isServerFinished());
            release.countDown();
            producer.join(2000);
            assertFalse(producer.isAlive());
            assertTrue(observer.isServerFinished());
            assertEquals(Integer.valueOf(7), observer.consume().next());
            assertEquals(null, observer.consume());
        } finally {
            release.countDown();
            producer.join(2000);
            observer.clear();
        }
    }

    @Test(timeout = 5000)
    public void testCloseDiscardsResponseBeingParsed() throws Exception {
        CountDownLatch parsing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CommonKvStreamObserver<List<Integer>, Integer> observer = parsingObserver(parsing, release);
        AtomicInteger cancellations = new AtomicInteger();
        observer.setTransferComplete(finished -> {
            assertFalse(finished);
            cancellations.incrementAndGet();
        });
        Thread producer = new Thread(() -> observer.onNext(Collections.singletonList(7)));
        producer.start();
        try {
            assertTrue(parsing.await(2, TimeUnit.SECONDS));
            observer.clear();
            observer.clear();
            assertEquals(null, observer.consume());
            release.countDown();
            producer.join(2000);
            assertFalse(producer.isAlive());
            observer.onCompleted();
            observer.onError(new IllegalStateException("late error"));
            assertEquals(null, observer.consume());
            assertEquals(1, cancellations.get());
        } finally {
            release.countDown();
            producer.join(2000);
        }
    }

    @Test(timeout = 5000)
    public void testCancellationBypassesBlockedRequestSender() throws Exception {
        CountDownLatch sending = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        CommonKvStreamObserver<List<Integer>, Integer> observer = observer(new RecordingQueue());
        observer.setRequestSender(ignored -> {
            sending.countDown();
            try {
                release.await();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(error);
            }
        });
        AtomicInteger cancellations = new AtomicInteger();
        observer.setTransferComplete(finished -> {
            assertFalse(finished);
            cancellations.incrementAndGet();
            cancelled.countDown();
            release.countDown();
        });
        Thread sender = new Thread(observer::sendRequest);
        Thread closer = new Thread(observer::clear);
        sender.start();
        try {
            assertTrue(sending.await(2, TimeUnit.SECONDS));
            closer.start();
            assertTrue("cancel must not wait for the blocked sender", cancelled.await(2, TimeUnit.SECONDS));
            sender.join(1000);
            closer.join(1000);
            assertFalse(sender.isAlive());
            assertFalse(closer.isAlive());
            assertEquals(1, cancellations.get());
        } finally {
            release.countDown();
            sender.join(1000);
            closer.join(1000);
        }
    }

    @Test
    public void testTerminalCallbacksAreIdempotentAndDoNotResurrectStream() throws Exception {
        CommonKvStreamObserver<List<Integer>, Integer> observer = observer(new RecordingQueue());
        observer.onError(new IllegalStateException("first error"));
        observer.onCompleted();
        observer.onError(new IllegalStateException("duplicate error"));
        observer.onNext(Collections.singletonList(7));
        assertTrue(observer.isServerFinished());
        Iterator<Integer> error = observer.consume();
        assertTrue(error instanceof ErrorMessageIterator);
        assertEquals(null, observer.consume());

        CommonKvStreamObserver<List<Integer>, Integer> completed = observer(new RecordingQueue());
        AtomicInteger halfCloses = new AtomicInteger();
        completed.setTransferComplete(finished -> {
            assertTrue(finished);
            halfCloses.incrementAndGet();
        });
        completed.onCompleted();
        completed.onCompleted();
        completed.onError(new IllegalStateException("late error"));
        completed.onNext(Collections.singletonList(7));
        completed.clear();
        completed.clear();
        assertEquals(1, halfCloses.get());
        assertEquals(null, completed.consume());
    }

    @Test(timeout = 5000)
    public void testErrorDiscardsResponseBeingParsed() throws Exception {
        CountDownLatch parsing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CommonKvStreamObserver<List<Integer>, Integer> observer = parsingObserver(parsing, release);
        Thread producer = new Thread(() -> observer.onNext(Collections.singletonList(7)));
        producer.start();
        try {
            assertTrue(parsing.await(2, TimeUnit.SECONDS));
            observer.onError(new IllegalStateException("transport error"));
            observer.onCompleted();
            assertTrue("error does not wait for a discarded batch", observer.isServerFinished());
            assertTrue(observer.consume() instanceof ErrorMessageIterator);
            assertEquals(null, observer.consume());
            release.countDown();
            producer.join(2000);
            assertFalse(producer.isAlive());
            assertEquals(null, observer.consume());
        } finally {
            release.countDown();
            producer.join(2000);
            observer.clear();
        }
    }

    @Test
    public void testParserErrorReleasesAcceptedResponse() {
        CommonKvStreamObserver<List<Integer>, Integer> observer = new CommonKvStreamObserver<>(values -> {
            throw new AssertionError("parser error");
        }, ignored -> ResultState.FINISHED);
        try {
            observer.onNext(Collections.singletonList(7));
            throw new AssertionError("parser Error must propagate");
        } catch (AssertionError error) {
            assertEquals("parser error", error.getMessage());
        }
        assertTrue(observer.isServerFinished());
        assertTrue(observer.consume() instanceof ErrorMessageIterator);
        assertEquals(null, observer.consume());
    }

    @Test(timeout = 5000)
    public void testParserErrorOverridesConcurrentCompletion() throws Exception {
        assertParserFailureOverridesCompletion(new AssertionError("parser error"));
    }

    @Test(timeout = 5000)
    public void testParserExceptionOverridesConcurrentCompletion() throws Exception {
        assertParserFailureOverridesCompletion(new IllegalStateException("parser exception"));
    }

    private static void assertParserFailureOverridesCompletion(Throwable failure) throws Exception {
        CountDownLatch parsing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CommonKvStreamObserver<List<Integer>, Integer> observer = new CommonKvStreamObserver<>(values -> {
            parsing.countDown();
            await(release);
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw (RuntimeException) failure;
        }, ignored -> ResultState.FINISHED);
        observer.setRequestSender(ignored -> { });
        observer.setTransferComplete(ignored -> { });
        AtomicReference<Throwable> propagated = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                observer.onNext(Collections.singletonList(7));
            } catch (Throwable error) {
                propagated.set(error);
            }
        });
        producer.start();
        try {
            assertTrue(parsing.await(2, TimeUnit.SECONDS));
            observer.onCompleted();
            assertFalse(observer.isServerFinished());
            release.countDown();
            producer.join(2000);
            assertFalse(producer.isAlive());
            assertEquals(failure instanceof Error ? failure : null, propagated.get());
            assertTrue(observer.isServerFinished());
            assertTrue("parser failure must remain visible to the consumer",
                       observer.consume() instanceof ErrorMessageIterator);
            assertEquals(null, observer.consume());
        } finally {
            release.countDown();
            producer.join(2000);
            observer.clear();
        }
    }

    private static CommonKvStreamObserver<List<Integer>, Integer> parsingObserver(
            CountDownLatch parsing, CountDownLatch release) {
        CommonKvStreamObserver<List<Integer>, Integer> observer = new CommonKvStreamObserver<>(values -> {
            parsing.countDown();
            await(release);
            return values.iterator();
        }, ignored -> ResultState.FINISHED);
        observer.setRequestSender(ignored -> { });
        observer.setTransferComplete(ignored -> { });
        return observer;
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(2, TimeUnit.SECONDS));
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new AssertionError(error);
        }
    }

    private static List<Integer> drain(HgKvIterator<Integer> stream) {
        List<Integer> values = new ArrayList<>();
        while (stream.hasNext()) {
            values.add(stream.next());
        }
        return values;
    }

    private static MultiStreamIterator<Integer> merged(List<RecordingQueue> queues) throws Exception {
        List<HgKvIterator<Integer>> streams = new ArrayList<>();
        for (int value : Arrays.asList(1, 2, 3)) {
            RecordingQueue queue = new RecordingQueue();
            queues.add(queue);
            CommonKvStreamObserver<List<Integer>, Integer> observer = observer(queue);
            observer.onNext(Collections.singletonList(value));
            observer.onCompleted();
            streams.add(stream(observer));
        }
        return new MultiStreamIterator<>(streams);
    }

    private static StreamKvIterator<Integer> stream(CommonKvStreamObserver<List<Integer>, Integer> observer) {
        return new StreamKvIterator<>(ignored -> observer.clear(), observer::consume);
    }

    private static CommonKvStreamObserver<List<Integer>, Integer> observer(RecordingQueue queue) throws Exception {
        CommonKvStreamObserver<List<Integer>, Integer> observer =
                new CommonKvStreamObserver<>(List::iterator, ignored -> ResultState.FINISHED);
        observer.setRequestSender(ignored -> { });
        observer.setTransferComplete(ignored -> { });
        Field field = CommonKvStreamObserver.class.getDeclaredField("queue");
        field.setAccessible(true);
        field.set(observer, queue);
        return observer;
    }

    private static class RecordingQueue extends LinkedBlockingQueue<Iterator<Integer>> {

        private static final long serialVersionUID = 1L;
        final AtomicInteger emptyTimedPolls = new AtomicInteger();

        @Override
        public Iterator<Integer> poll(long timeout, TimeUnit unit) throws InterruptedException {
            if (super.isEmpty()) {
                this.emptyTimedPolls.incrementAndGet();
            }
            return super.poll(timeout, unit);
        }
    }

    private static final class GatedQueue extends RecordingQueue {

        private static final long serialVersionUID = 1L;
        private final CountDownLatch boundary;
        private final CountDownLatch published;
        private final AtomicBoolean armed = new AtomicBoolean(true);

        private GatedQueue(CountDownLatch boundary, CountDownLatch published) {
            this.boundary = boundary;
            this.published = published;
        }

        private void publishAtBoundary() {
            if (this.armed.compareAndSet(true, false)) {
                this.boundary.countDown();
                try {
                    assertTrue("final batch must be published", this.published.await(2, TimeUnit.SECONDS));
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(error);
                }
            }
        }

        @Override
        public boolean isEmpty() {
            boolean empty = super.isEmpty();
            this.publishAtBoundary();
            return empty;
        }

        @Override
        public Iterator<Integer> poll(long timeout, TimeUnit unit) throws InterruptedException {
            this.publishAtBoundary();
            return super.poll(timeout, unit);
        }
    }
}
