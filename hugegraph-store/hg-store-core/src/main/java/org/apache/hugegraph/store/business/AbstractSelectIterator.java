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

package org.apache.hugegraph.store.business;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.structure.BaseElement;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSelectIterator implements ScanIterator {

    protected ScanIterator iterator;
    protected BinaryElementSerializer serializer;

    public AbstractSelectIterator() {
        this.serializer = new BinaryElementSerializer();
    }

    public BaseElement parseEntry(BackendColumn column, boolean isVertex) {
        if (column == null) {
            throw new IllegalArgumentException("BackendColumn cannot be null");
        }
        if (isVertex) {
            return serializer.parseVertex(null, column, null);
        } else {
            return serializer.parseEdge(null, column, null, true);
        }
    }
}
