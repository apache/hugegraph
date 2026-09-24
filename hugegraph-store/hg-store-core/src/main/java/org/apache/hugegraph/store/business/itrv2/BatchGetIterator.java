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

import java.util.Iterator;
import java.util.function.Function;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.query.QueryTypeParam;

/**
 * Query data by multiple ids, return an iterator
 * ID query
 */
public class BatchGetIterator implements ScanIterator {

    private final Iterator<QueryTypeParam> iterator;

    private final Function<QueryTypeParam, byte[]> retriveFunction;

    private byte[] pos;

    public BatchGetIterator(Iterator<QueryTypeParam> iterator,
                            Function<QueryTypeParam, byte[]> retriveFunction) {
        this.iterator = iterator;
        this.retriveFunction = retriveFunction;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public boolean isValid() {
        return this.iterator.hasNext();
    }

    @Override
    public RocksDBSession.BackendColumn next() {
        var param = iterator.next();
        byte[] key = param.getStart();
        this.pos = key;
        var value = retriveFunction.apply(param);
        return value == null ? null : RocksDBSession.BackendColumn.of(key, value);
    }

    @Override
    public void close() {

    }

    @Override
    public byte[] position() {
        return this.pos;
    }

    @Override
    public long count() {
        long count = 0L;
        while (this.iterator.hasNext()) {
            this.iterator.next();
            count += 1;
        }
        return count;
    }

    @Override
    public void seek(byte[] position) {
        // not supported
    }
}
