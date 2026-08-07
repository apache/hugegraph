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

package org.apache.hugegraph.task;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Function;

import org.apache.hugegraph.task.TaskResultPageCursor.RootType;
import org.apache.hugegraph.task.TaskResultStreamException.Reason;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;

public final class TaskResultStreamer {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final int BUFFER_SIZE = 8192;

    private TaskResultStreamer() {
    }

    public static void stream(TaskResultSnapshot snapshot, OutputStream output,
                              long deadlineNanos) throws IOException {
        E.checkNotNull(snapshot, "task result snapshot");
        E.checkNotNull(output, "output");
        checkDeadline(deadlineNanos);
        try (InputStream input = snapshot.openResultStream()) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int length;
            while ((length = input.read(buffer)) != -1) {
                checkDeadline(deadlineNanos);
                output.write(buffer, 0, length);
                checkDeadline(deadlineNanos);
            }
            checkDeadline(deadlineNanos);
            output.flush();
            checkDeadline(deadlineNanos);
        }
    }

    public static RootType preflight(TaskResultSnapshot snapshot, long offset,
                                     long maxScanBytes, long deadlineNanos) {
        return preflight(snapshot, offset, 1, maxScanBytes, deadlineNanos);
    }

    public static RootType preflight(TaskResultSnapshot snapshot, long offset,
                                     int pageSize, long maxScanBytes,
                                     long deadlineNanos) {
        E.checkArgument(pageSize > 0, "The page size must be positive");
        try (ScanCursor cursor = openAtOffset(snapshot, null, offset,
                                              maxScanBytes, deadlineNanos)) {
            int inspected = 0;
            while (inspected < pageSize && cursor.hasCurrentItem()) {
                cursor.skipCurrentItem();
                cursor.moveNext();
                inspected++;
            }
            return cursor.rootType;
        } catch (IOException e) {
            throw invalidJson(e);
        }
    }

    public static void streamPage(TaskResultSnapshot snapshot,
                                  RootType rootType, long offset,
                                  int pageSize, long maxScanBytes,
                                  long deadlineNanos, OutputStream output,
                                  Function<TaskResultPageCursor, String>
                                  tokenEncoder) throws IOException {
        E.checkNotNull(output, "output");
        E.checkNotNull(tokenEncoder, "page token encoder");
        E.checkArgument(pageSize > 0, "The page size must be positive");

        try (ScanCursor cursor = openAtOffset(snapshot, rootType, offset,
                                              maxScanBytes, deadlineNanos)) {
            JsonGenerator generator = JSON_FACTORY.createGenerator(output);
            generator.writeStartObject();
            generator.writeStringField("root_type", rootType.text());
            generator.writeArrayFieldStart("items");

            int emitted = 0;
            while (emitted < pageSize && cursor.hasCurrentItem()) {
                checkDeadline(deadlineNanos);
                if (rootType == RootType.ARRAY) {
                    generator.copyCurrentStructure(cursor.parser);
                } else {
                    String key = cursor.parser.currentName();
                    cursor.requireNextValue();
                    generator.writeStartObject();
                    generator.writeStringField("key", key);
                    generator.writeFieldName("value");
                    generator.copyCurrentStructure(cursor.parser);
                    generator.writeEndObject();
                }
                emitted++;
                cursor.moveNext();
                checkDeadline(deadlineNanos);
            }

            generator.writeEndArray();
            if (cursor.hasCurrentItem()) {
                TaskResultPageCursor next = new TaskResultPageCursor(
                        rootType, nextOffset(offset, emitted), pageSize,
                        snapshot.fingerprint());
                generator.writeStringField("next_page",
                                           tokenEncoder.apply(next));
            } else {
                generator.writeNullField("next_page");
            }
            generator.writeEndObject();
            checkDeadline(deadlineNanos);
            generator.flush();
            checkDeadline(deadlineNanos);
        }
    }

    private static ScanCursor openAtOffset(TaskResultSnapshot snapshot,
                                           RootType expectedRoot,
                                           long offset, long maxScanBytes,
                                           long deadlineNanos)
                                           throws IOException {
        E.checkNotNull(snapshot, "task result snapshot");
        E.checkArgument(offset >= 0L, "The offset must be non-negative");
        E.checkArgument(maxScanBytes >= 0L,
                        "The scan byte limit must be non-negative");
        checkDeadline(deadlineNanos);

        LimitedInputStream input = new LimitedInputStream(
                snapshot.openResultStream(), maxScanBytes, deadlineNanos);
        JsonParser parser = null;
        try {
            parser = JSON_FACTORY.createParser(input);
            JsonToken rootToken = parser.nextToken();
            RootType rootType = rootType(rootToken);
            if (expectedRoot != null && rootType != expectedRoot) {
                throw new TaskResultStreamException(
                        Reason.ROOT_MISMATCH,
                        "The task result root changed from '%s' to '%s'",
                        expectedRoot.text(), rootType.text());
            }
            ScanCursor cursor = new ScanCursor(parser, rootType,
                                               deadlineNanos);
            cursor.moveNext();
            for (long index = 0L; index < offset; index++) {
                if (!cursor.hasCurrentItem()) {
                    throw new TaskResultStreamException(
                            Reason.INVALID_OFFSET,
                            "The page offset '%s' exceeds the result size",
                            offset);
                }
                cursor.skipCurrentItem();
                cursor.moveNext();
            }
            return cursor;
        } catch (RuntimeException | IOException e) {
            if (parser != null) {
                parser.close();
            } else {
                input.close();
            }
            throw e;
        }
    }

    private static RootType rootType(JsonToken token) {
        if (token == JsonToken.START_ARRAY) {
            return RootType.ARRAY;
        }
        if (token == JsonToken.START_OBJECT) {
            return RootType.OBJECT;
        }
        throw new TaskResultStreamException(
                Reason.NOT_PAGEABLE,
                "Only a top-level JSON array or object can be paginated");
    }

    private static void checkDeadline(long deadlineNanos) {
        if (System.nanoTime() > deadlineNanos) {
            throw new TaskResultStreamException(
                    Reason.TIMEOUT, "Reading the task result timed out");
        }
    }

    private static TaskResultStreamException invalidJson(IOException cause) {
        return new TaskResultStreamException(
                Reason.INVALID_JSON, cause,
                "The persisted task result is not valid JSON");
    }

    private static long nextOffset(long offset, int emitted) {
        try {
            return Math.addExact(offset, emitted);
        } catch (ArithmeticException e) {
            throw new TaskResultStreamException(
                    Reason.INVALID_OFFSET, e,
                    "The task result page offset overflowed");
        }
    }

    private static final class ScanCursor implements AutoCloseable {

        private final JsonParser parser;
        private final RootType rootType;
        private final long deadlineNanos;
        private JsonToken current;

        private ScanCursor(JsonParser parser, RootType rootType,
                           long deadlineNanos) {
            this.parser = parser;
            this.rootType = rootType;
            this.deadlineNanos = deadlineNanos;
            this.current = null;
            this.checkLimits();
        }

        private boolean hasCurrentItem() {
            JsonToken end = this.rootType == RootType.ARRAY ?
                            JsonToken.END_ARRAY : JsonToken.END_OBJECT;
            return this.current != null && this.current != end;
        }

        private void requireNextValue() throws IOException {
            JsonToken token = this.parser.nextToken();
            if (token == null) {
                throw new TaskResultStreamException(
                        Reason.INVALID_JSON,
                        "The persisted task result ended after a field name");
            }
            this.checkLimits();
        }

        private void skipCurrentItem() throws IOException {
            if (this.rootType == RootType.OBJECT) {
                this.requireNextValue();
            }
            this.parser.skipChildren();
            this.checkLimits();
        }

        private void moveNext() throws IOException {
            this.current = this.parser.nextToken();
            this.checkLimits();
        }

        private void checkLimits() {
            checkDeadline(this.deadlineNanos);
        }

        @Override
        public void close() throws IOException {
            this.parser.close();
        }
    }

    private static final class LimitedInputStream extends InputStream {

        private final InputStream input;
        private final long maxBytes;
        private final long deadlineNanos;
        private long count;

        private LimitedInputStream(InputStream input, long maxBytes,
                                   long deadlineNanos) {
            this.input = input;
            this.maxBytes = maxBytes;
            this.deadlineNanos = deadlineNanos;
            this.count = 0L;
        }

        @Override
        public int read() throws IOException {
            checkDeadline(this.deadlineNanos);
            if (this.count >= this.maxBytes) {
                return this.rejectMoreBytes(this.input.read());
            }
            int value = this.input.read();
            if (value != -1) {
                this.count++;
            }
            checkDeadline(this.deadlineNanos);
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length)
                        throws IOException {
            checkDeadline(this.deadlineNanos);
            if (length == 0) {
                return 0;
            }
            long remaining = this.maxBytes - this.count;
            if (remaining <= 0L) {
                return this.rejectMoreBytes(this.input.read());
            }
            int allowed = (int) Math.min((long) length, remaining);
            int read = this.input.read(bytes, offset, allowed);
            if (read > 0) {
                this.count += read;
            }
            checkDeadline(this.deadlineNanos);
            return read;
        }

        @Override
        public void close() throws IOException {
            this.input.close();
        }

        private int rejectMoreBytes(int value) {
            if (value == -1) {
                return -1;
            }
            throw new TaskResultStreamException(
                    Reason.SCAN_LIMIT_EXCEEDED,
                    "Scanning the task result exceeded '%s' bytes",
                    this.maxBytes);
        }
    }
}
