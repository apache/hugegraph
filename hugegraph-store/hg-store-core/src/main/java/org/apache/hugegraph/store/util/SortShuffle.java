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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.business.itrv2.FileObjectIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;

public class SortShuffle<T extends Serializable> implements AutoCloseable {

    private static final int BATCH_SIZE = 1000000;

    private static final int FILE_SIZE = 3;

    private static final AtomicInteger fileSeq = new AtomicInteger(0);

    private static String basePath =
            System.getProperty("java.io.tmpdir") +
            (System.getProperty("java.io.tmpdir").endsWith(File.separator) ? "" : File.separator);

    private final String path;

    private final Queue<T> queue = new ConcurrentLinkedDeque<>();

    private final Comparator<T> comparator;

    private final SortShuffleSerializer<T> serializer;

    private final Deque<String> files = new ArrayDeque<>();

    private final List<FileObjectIterator<T>> iterators = new ArrayList<>();

    public SortShuffle(Comparator<T> comparator, SortShuffleSerializer<T> serializer) {
        this.comparator = comparator;
        path = basePath + Thread.currentThread().getId() + "-" +
               System.currentTimeMillis() % 10000 + "/";
        new File(path).mkdirs();
        this.serializer = serializer;
    }

    public static String getBasePath() {
        return basePath;
    }

    public static void setBasePath(String path) {
        basePath = path;
    }

    /**
     * Append object t to the file. If the record count in the file has reached BATCH_SIZE, write
     * it to the file and clear the queue
     *
     * @param t Object to append
     * @throws IOException
     */
    public void append(T t) throws IOException {
        if (queue.size() >= BATCH_SIZE) {
            synchronized (this) {
                if (queue.size() >= BATCH_SIZE) {
                    writeToFile();
                    queue.clear();
                }
            }
        }
        queue.add(t);
    }

    public void finish() throws IOException {
        finalMerge();
    }

    /**
     * Delete file/directory and close resource
     */
    @Override
    public void close() {
        this.iterators.forEach(FileObjectIterator::close);
        this.iterators.clear();
        if (this.files.size() > 0) {
            while (this.files.size() > 0) {
                new File(files.pop()).delete();
            }
        }
        new File(path).delete();
        this.files.clear();
        this.queue.clear();
    }

    /**
     * Write data to file
     *
     * @throws IOException throw exception when write file or create a directory
     */
    private void writeToFile() throws IOException {
        if (!new File(path).exists()) {
            new File(path).mkdirs();
        }

        if (files.size() >= FILE_SIZE) {
            minorMerge(files.pop(), files.pop());
        }

        var fn = getFileName();
        try (OutputStream fos = new FileOutputStream(fn)) {
            Iterator<T> sorted = queue.stream().sorted(this.comparator).iterator();
            while (sorted.hasNext()) {
                serializer.write(fos, sorted.next());
            }
        } catch (IOException | RuntimeException e) {
            new File(fn).delete();
            throw e;
        }
        this.files.add(fn);
    }

    private synchronized String getFileName() {
        return path + fileSeq.getAndIncrement();
    }

    /**
     * merge with file data when spill files exceed FILE_SIZE
     */
    private void minorMerge(String f1, String f2) throws IOException {
        String fn = getFileName();
        try (OutputStream fos = new FileOutputStream(fn);
             InputStream fis1 = new FileInputStream(f1);
             InputStream fis2 = new FileInputStream(f2)) {
            T o1 = serializer.read(fis1);
            T o2 = serializer.read(fis2);

            // read sorted fn1 and f2, write to new file
            while (o1 != null && o2 != null) {
                if (comparator.compare(o1, o2) < 0) {
                    serializer.write(fos, o1);
                    o1 = serializer.read(fis1);
                } else {
                    serializer.write(fos, o2);
                    o2 = serializer.read(fis2);
                }
            }

            if (o1 != null) {
                serializer.write(fos, o1);
                while ((o1 = serializer.read(fis1)) != null) {
                    serializer.write(fos, o1);
                }
            }

            if (o2 != null) {
                serializer.write(fos, o2);
                while ((o2 = serializer.read(fis2)) != null) {
                    serializer.write(fos, o2);
                }
            }
        } catch (IOException | RuntimeException e) {
            new File(fn).delete();
            this.files.addFirst(f2);
            this.files.addFirst(f1);
            throw e;
        }

        new File(f1).delete();
        new File(f2).delete();
        files.add(fn);
    }

    /**
     * merge all split files
     */
    private void finalMerge() throws IOException {

        if (this.files.size() == 0) {
            return;
        }

        writeToFile();
        queue.clear();

        while (this.files.size() > 1) {
            minorMerge(this.files.pop(), this.files.pop());
        }
    }

    /**
     * read all sorted element
     *
     * @return iterator
     */
    public Iterator<T> getIterator() throws IOException {
        if (files.size() == 0) {
            return queue.stream().sorted(this.comparator).iterator();
        }

        FileObjectIterator<T> iterator = new FileObjectIterator<>(files.getFirst(), this.serializer);
        this.iterators.add(iterator);
        return iterator;
    }

}
