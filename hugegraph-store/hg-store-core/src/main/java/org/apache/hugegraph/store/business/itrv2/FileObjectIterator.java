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

package org.apache.hugegraph.store.business.itrv2;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileObjectIterator<T> implements Iterator<T>, AutoCloseable {

    private FileInputStream fis = null;
    private T current;
    private boolean closed;
    private final String fn;
    private final SortShuffleSerializer<T> serializer;

    public FileObjectIterator(String filePath, SortShuffleSerializer<T> serializer) {
        this.fn = filePath;
        this.serializer = serializer;
    }

    @Override
    public boolean hasNext() {
        if (this.current != null) {
            return true;
        }
        if (this.closed) {
            return false;
        }
        try {
            if (fis == null) {
                fis = new FileInputStream(this.fn);
            }
            current = readObject(fis);
            if (current == null) {
                close();
            }
            return current != null;
        } catch (IOException e) {
            close();
            throw new UncheckedIOException("Failed to read object from file " + this.fn, e);
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T result = this.current;
        this.current = null;
        return result;
    }

    @Override
    public void close() {
        this.closed = true;
        this.current = null;
        if (this.fis != null) {
            try {
                this.fis.close();
            } catch (IOException e) {
                log.warn("Failed to close file stream", e);
            } finally {
                this.fis = null;
            }
        }
    }

    public T readObject(InputStream input) throws IOException {
        return serializer.read(input);
    }
}
