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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.NoSuchElementException;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.store.business.itrv2.FileObjectIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileObjectIteratorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRepeatedHasNextAndDirectNextKeepEveryRecord() throws Exception {
        try (FileObjectIterator<BackendColumn> iterator = iterator(file(1, 2, 3))) {
            assertTrue(iterator.hasNext());
            assertTrue(iterator.hasNext());
            assertEquals(1, iterator.next().name[0]);
            assertEquals(2, iterator.next().name[0]);
            assertTrue(iterator.hasNext());
            assertEquals(3, iterator.next().name[0]);
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
    }

    @Test
    public void testEofClosesFileAndDoesNotReopenIt() throws Exception {
        try (FileObjectIterator<BackendColumn> iterator = iterator(file(1))) {
            assertEquals(1, iterator.next().name[0]);
            FileInputStream stream = stream(iterator);
            assertTrue(stream.getFD().valid());

            assertFalse(iterator.hasNext());
            assertFalse(stream.getFD().valid());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testCloseDiscardsPrefetchedRecordAndClosesFile() throws Exception {
        try (FileObjectIterator<BackendColumn> iterator = iterator(file(1, 2))) {
            assertTrue(iterator.hasNext());
            FileInputStream stream = stream(iterator);
            iterator.close();
            iterator.close();

            assertFalse(stream.getFD().valid());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
    }

    @Test
    public void testEmptyFileRemainsExhausted() throws Exception {
        try (FileObjectIterator<BackendColumn> iterator = iterator(file())) {
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
    }

    @Test
    public void testTruncatedFileFailsInsteadOfReturningPartialResults() throws Exception {
        File file = file(1);
        Files.write(file.toPath(), new byte[]{0, 0});
        try (FileObjectIterator<BackendColumn> iterator = iterator(file)) {
            assertThrows(UncheckedIOException.class, iterator::hasNext);
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testReadFailureClosesFileAndRemainsTerminal() throws Exception {
        FileInputStream[] opened = new FileInputStream[1];
        try (FileObjectIterator<BackendColumn> iterator = new FileObjectIterator<>(
                file(1).getAbsolutePath(), SortShuffleSerializer.ofBackendColumnSerializer()) {
            @Override
            public BackendColumn readObject(InputStream input) throws IOException {
                opened[0] = (FileInputStream) input;
                throw new IOException("read failure");
            }
        }) {
            UncheckedIOException failure = assertThrows(UncheckedIOException.class, iterator::hasNext);
            assertEquals("read failure", failure.getCause().getMessage());
            assertFalse(opened[0].getFD().valid());
            assertFalse(iterator.hasNext());
        }
    }

    private File file(int... keys) throws IOException {
        File file = this.folder.newFile();
        try (OutputStream output = Files.newOutputStream(file.toPath())) {
            for (int key : keys) {
                SortShuffleSerializer.ofBackendColumnSerializer().write(
                        output, BackendColumn.of(new byte[]{(byte) key}, new byte[]{1}));
            }
        }
        return file;
    }

    private static FileObjectIterator<BackendColumn> iterator(File file) {
        return new FileObjectIterator<>(file.getAbsolutePath(),
                                        SortShuffleSerializer.ofBackendColumnSerializer());
    }

    private static FileInputStream stream(FileObjectIterator<?> iterator) throws Exception {
        Field field = FileObjectIterator.class.getDeclaredField("fis");
        field.setAccessible(true);
        return (FileInputStream) field.get(iterator);
    }
}
