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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileObjectIterator<T> implements Iterator<T> {

    private FileInputStream fis = null;
    private T current;
    private final String fn;
    private final SortShuffleSerializer<T> serializer;

    public FileObjectIterator(String filePath, SortShuffleSerializer<T> serializer) {
        this.fn = filePath;
        this.serializer = serializer;
    }

    @Override
    public boolean hasNext() {
        try {
            if (fis == null) {
                fis = new FileInputStream(this.fn);
            }
            current = readObject(fis);

            if (current != null) {
                return true;
            } else {
                String parent = new File(this.fn).getParent();
                new File(parent).delete();
            }
        } catch (Exception e) {
            log.error("Failed to read object from file", e);
            if (fis != null) {
                try {
                    fis.close();
                    fis = null;
                } catch (IOException ex) {
                    log.warn("Failed to close file stream during error handling", ex);
                }

            }
        }
        return false;
    }

    @Override
    public T next() {
        return current;
    }

    public T readObject(InputStream input) throws IOException {
        return serializer.read(input);
    }
}
