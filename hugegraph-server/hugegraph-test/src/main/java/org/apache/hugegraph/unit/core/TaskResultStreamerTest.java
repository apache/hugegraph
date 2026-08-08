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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.task.TaskResultPageCursor;
import org.apache.hugegraph.task.TaskResultPageCursor.RootType;
import org.apache.hugegraph.task.TaskResultSnapshot;
import org.apache.hugegraph.task.TaskResultStreamException;
import org.apache.hugegraph.task.TaskResultStreamException.Reason;
import org.apache.hugegraph.task.TaskResultStreamer;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.StringEncoding;
import org.junit.Test;

public class TaskResultStreamerTest {

    @Test
    public void testStreamCompleteResultWithoutChangingJson() throws Exception {
        String json = "[1,{\"name\":\"marko\"},true,null]";
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        TaskResultStreamer.stream(snapshot(json), output, Long.MAX_VALUE);

        Assert.assertEquals(json, utf8(output));
    }

    @Test
    public void testStreamArrayPageAndContinuation() throws Exception {
        TaskResultSnapshot snapshot = snapshot("[1,{\"name\":\"marko\"},3,4]");
        RootType rootType = TaskResultStreamer.preflight(
                snapshot, 1L, 1024L, Long.MAX_VALUE);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        AtomicReference<TaskResultPageCursor> cursor = new AtomicReference<>();

        TaskResultStreamer.streamPage(snapshot, rootType, 1L, 2, 1024L,
                                      Long.MAX_VALUE, output,
                                      value -> {
                                          cursor.set(value);
                                          return "next-page";
                                      });

        Assert.assertEquals(RootType.ARRAY, rootType);
        Assert.assertEquals("{\"root_type\":\"array\",\"items\":[" +
                            "{\"name\":\"marko\"},3]," +
                            "\"page\":\"next-page\"}", utf8(output));
        Assert.assertEquals(3L, cursor.get().nextOffset());
        Assert.assertEquals(2, cursor.get().pageSize());
        Assert.assertEquals(snapshot.fingerprint(),
                            cursor.get().fingerprint());
    }

    @Test
    public void testStreamFinalArrayPage() throws Exception {
        TaskResultSnapshot snapshot = snapshot("[1,2,3,4]");
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        TaskResultStreamer.streamPage(snapshot, RootType.ARRAY, 3L, 2,
                                      1024L, Long.MAX_VALUE, output,
                                      cursor -> "unexpected");

        Assert.assertEquals("{\"root_type\":\"array\",\"items\":[4]," +
                            "\"page\":null}", utf8(output));
    }

    @Test
    public void testStreamEmptyArrayAndObjectPages() throws Exception {
        TaskResultSnapshot array = snapshot("[]");
        ByteArrayOutputStream arrayOutput = new ByteArrayOutputStream();
        RootType arrayRoot = TaskResultStreamer.preflight(
                array, 0L, 1024L, Long.MAX_VALUE);

        TaskResultStreamer.streamPage(array, arrayRoot, 0L, 2, 1024L,
                                      Long.MAX_VALUE, arrayOutput,
                                      cursor -> "unexpected");

        Assert.assertEquals("{\"root_type\":\"array\",\"items\":[]," +
                            "\"page\":null}", utf8(arrayOutput));

        TaskResultSnapshot object = snapshot("{}");
        ByteArrayOutputStream objectOutput = new ByteArrayOutputStream();
        RootType objectRoot = TaskResultStreamer.preflight(
                object, 0L, 1024L, Long.MAX_VALUE);

        TaskResultStreamer.streamPage(object, objectRoot, 0L, 2, 1024L,
                                      Long.MAX_VALUE, objectOutput,
                                      cursor -> "unexpected");

        Assert.assertEquals("{\"root_type\":\"object\",\"items\":[]," +
                            "\"page\":null}", utf8(objectOutput));
    }

    @Test
    public void testStreamObjectPagePreservesDuplicateKeys() throws Exception {
        TaskResultSnapshot snapshot = snapshot("{\"a\":1,\"b\":{\"x\":2},\"a\":3}");
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        TaskResultStreamer.streamPage(snapshot, RootType.OBJECT, 1L, 2,
                                      1024L, Long.MAX_VALUE, output,
                                      cursor -> "unexpected");

        Assert.assertEquals("{\"root_type\":\"object\",\"items\":[" +
                            "{\"key\":\"b\",\"value\":{\"x\":2}}," +
                            "{\"key\":\"a\",\"value\":3}]," +
                            "\"page\":null}", utf8(output));
    }

    @Test
    public void testRejectScalarPagination() {
        TaskResultStreamException exception = Assert.assertThrows(
                TaskResultStreamException.class,
                () -> TaskResultStreamer.preflight(snapshot("3"), 0L,
                                                   1024L, Long.MAX_VALUE));

        Assert.assertEquals(Reason.NOT_PAGEABLE, exception.reason());
    }

    @Test
    public void testRejectOffsetPastEnd() {
        TaskResultStreamException exception = Assert.assertThrows(
                TaskResultStreamException.class,
                () -> TaskResultStreamer.preflight(snapshot("[1,2]"), 3L,
                                                   1024L, Long.MAX_VALUE));

        Assert.assertEquals(Reason.INVALID_OFFSET, exception.reason());
    }

    @Test
    public void testRejectScanOverByteBudget() {
        TaskResultStreamException exception = Assert.assertThrows(
                TaskResultStreamException.class,
                () -> TaskResultStreamer.preflight(snapshot("[1,2,3]"), 2L,
                                                   1L, Long.MAX_VALUE));

        Assert.assertEquals(Reason.SCAN_LIMIT_EXCEEDED, exception.reason());
    }

    @Test
    public void testPreflightScansCurrentPageWithinByteBudget() {
        long maxScanBytes = 64L * 1024L;
        String json = "[\"" + repeat('x', 1024 * 1024) + "\"]";

        TaskResultStreamException exception = Assert.assertThrows(
                TaskResultStreamException.class,
                () -> TaskResultStreamer.preflight(snapshot(json), 0L,
                                                   maxScanBytes,
                                                   Long.MAX_VALUE));

        Assert.assertEquals(Reason.SCAN_LIMIT_EXCEEDED, exception.reason());
    }

    @Test
    public void testPreflightScansEveryItemInSelectedPage() {
        long maxScanBytes = 64L * 1024L;
        String json = "[1,\"" + repeat('x', 1024 * 1024) + "\",3]";

        TaskResultStreamException exception = Assert.assertThrows(
                TaskResultStreamException.class,
                () -> TaskResultStreamer.preflight(snapshot(json), 0L, 2,
                                                   maxScanBytes,
                                                   Long.MAX_VALUE));

        Assert.assertEquals(Reason.SCAN_LIMIT_EXCEEDED, exception.reason());
    }

    @Test
    public void testStreamPageStopsWritingAtScanByteBudget() {
        long maxScanBytes = 64L * 1024L;
        String json = "[\"" + repeat('x', 1024 * 1024) + "\"]";
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        TaskResultStreamException exception = Assert.assertThrows(
                TaskResultStreamException.class,
                () -> TaskResultStreamer.streamPage(
                        snapshot(json), RootType.ARRAY, 0L, 1,
                        maxScanBytes, Long.MAX_VALUE, output,
                        cursor -> "unexpected"));

        Assert.assertEquals(Reason.SCAN_LIMIT_EXCEEDED, exception.reason());
        Assert.assertTrue(output.size() < maxScanBytes * 2L);
    }

    @Test
    public void testRejectExpiredDeadline() {
        TaskResultStreamException exception = Assert.assertThrows(
                TaskResultStreamException.class,
                () -> TaskResultStreamer.preflight(snapshot("[1]"), 0L,
                                                   1024L, 0L));

        Assert.assertEquals(Reason.TIMEOUT, exception.reason());
    }

    @Test
    public void testRejectStreamWhenWriteReturnsAfterDeadline() {
        long deadline = System.nanoTime() +
                        TimeUnit.MILLISECONDS.toNanos(10L);

        TaskResultStreamException exception = Assert.assertThrows(
                TaskResultStreamException.class,
                () -> TaskResultStreamer.stream(snapshot("[1]"),
                                                new SlowOutputStream(50L),
                                                deadline));

        Assert.assertEquals(Reason.TIMEOUT, exception.reason());
    }

    @Test
    public void testRejectInvalidJsonDuringPreflight() {
        TaskResultStreamException exception = Assert.assertThrows(
                TaskResultStreamException.class,
                () -> TaskResultStreamer.preflight(snapshot("["), 0L,
                                                   1024L, Long.MAX_VALUE));

        Assert.assertEquals(Reason.INVALID_JSON, exception.reason());
    }

    private static TaskResultSnapshot snapshot(String json) {
        return new TaskResultSnapshot(IdGenerator.of(123L), TaskStatus.SUCCESS,
                                      StringEncoding.compress(json));
    }

    private static String utf8(ByteArrayOutputStream output) {
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }

    private static final class SlowOutputStream extends OutputStream {

        private final long delayMillis;

        private SlowOutputStream(long delayMillis) {
            this.delayMillis = delayMillis;
        }

        @Override
        public void write(int value) throws IOException {
            this.delay();
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
                          throws IOException {
            this.delay();
        }

        private void delay() throws IOException {
            try {
                Thread.sleep(this.delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while writing", e);
            }
        }
    }
}
