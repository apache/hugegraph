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

package org.apache.hugegraph.store.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.junit.Test;

public class SortShuffleSerializerTest {

    private final SortShuffleSerializer<BackendColumn> serializer =
            SortShuffleSerializer.ofBackendColumnSerializer();

    @Test
    public void testRoundTripPreservesFormatAndReadsShortChunks() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.write(output, BackendColumn.of(new byte[]{1, 2}, new byte[]{3}));
        byte[] bytes = output.toByteArray();
        assertArrayEquals(new byte[]{0, 0, 0, 11, 0, 0, 0, 2, 1, 2, 0, 0, 0, 1, 3}, bytes);
        InputStream input = new ByteArrayInputStream(bytes) {
            @Override
            public synchronized int read(byte[] buffer, int offset, int length) {
                return super.read(buffer, offset, Math.min(length, 1));
            }
        };

        BackendColumn result = serializer.read(input);
        assertArrayEquals(new byte[]{1, 2}, result.name);
        assertArrayEquals(new byte[]{3}, result.value);
        assertNull(serializer.read(input));
    }

    @Test
    public void testTruncatedRecordsFailAtEveryByteBoundary() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.write(output, BackendColumn.of(new byte[]{1, 2}, new byte[]{3}));
        byte[] bytes = output.toByteArray();
        for (int length = 1; length < bytes.length; length++) {
            byte[] truncated = Arrays.copyOf(bytes, length);
            UncheckedIOException failure = assertThrows(UncheckedIOException.class,
                    () -> serializer.read(new ByteArrayInputStream(truncated)));
            assertSame(EOFException.class, failure.getCause().getClass());
        }
        assertNull(serializer.read(new ByteArrayInputStream(new byte[0])));
    }

    @Test
    public void testInvalidLengthAndIoFailureArePropagated() {
        assertThrows(UncheckedIOException.class,
                     () -> serializer.read(new ByteArrayInputStream(new byte[]{-1, -1, -1, -1})));
        IOException cause = new IOException("read failure");
        InputStream input = new InputStream() {
            @Override
            public int read() throws IOException {
                throw cause;
            }
        };
        UncheckedIOException failure = assertThrows(UncheckedIOException.class,
                                                    () -> serializer.read(input));
        assertSame(cause, failure.getCause());
    }
}
