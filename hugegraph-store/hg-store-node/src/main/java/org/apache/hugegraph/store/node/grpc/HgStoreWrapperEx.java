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

package org.apache.hugegraph.store.node.grpc;

import static org.apache.hugegraph.store.grpc.common.GraphMethod.GRAPH_METHOD_DELETE;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.FilterIterator;
import org.apache.hugegraph.store.grpc.common.GraphMethod;
import org.apache.hugegraph.store.grpc.common.TableMethod;
import org.apache.hugegraph.store.grpc.session.BatchEntry;
import org.apache.hugegraph.store.term.HgPair;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HgStoreWrapperEx {

    private final BusinessHandler handler;

    public HgStoreWrapperEx(BusinessHandler handler) {
        this.handler = handler;
    }

    public byte[] doGet(String graph, int code, String table, byte[] key) {
        return this.handler.doGet(graph, code, table, key);
    }

    public boolean doClean(String graph, int partId) {
        return this.handler.cleanPartition(graph, partId);
    }

    public ScanIterator scanAll(String graph, String table, byte[] query) {
        ScanIterator scanIterator = this.handler.scanAll(graph, table, query);
        return FilterIterator.of(scanIterator, query);
    }

    public ScanIterator scan(String graph, int partId, String table, byte[] start, byte[] end,
                             int scanType,
                             byte[] query) {
        ScanIterator scanIterator =
                this.handler.scan(graph, partId, table, start, end, scanType, query);
        return FilterIterator.of(scanIterator, query);
    }

    public ScanIterator scanOrdered(String graph, String table, byte[] start,
                                    byte[] end, int scanType, byte[] query) {
        ScanIterator scanIterator =
                this.handler.scanOrdered(graph, table, start, end, scanType);
        return FilterIterator.of(scanIterator, query);
    }

    public void batchGet(String graph, String table, Supplier<HgPair<Integer, byte[]>> s,
                         Consumer<HgPair<byte[], byte[]>> c) {
        this.handler.batchGet(graph, table, s, (pair -> {
            c.accept(new HgPair<>(pair.getKey(), pair.getValue()));
        }));
    }

    public ScanIterator scanPrefix(String graph, int partition, String table, byte[] prefix,
                                   int scanType,
                                   byte[] query) {
        ScanIterator scanIterator =
                this.handler.scanPrefix(graph, partition, table, prefix, scanType);
        return FilterIterator.of(scanIterator, query);
    }

    public void doBatch(String graph, int partId, List<BatchEntry> entryList) {
        this.handler.doBatch(graph, partId, entryList);
    }

    public boolean doTable(int partId, TableMethod method, String graph, String table) {
        boolean flag;
        switch (method) {
            case TABLE_METHOD_EXISTS:
                flag = this.handler.existsTable(graph, partId, table);
                break;
            case TABLE_METHOD_CREATE:
                this.handler.createTable(graph, partId, table);
                flag = true;
                break;
            case TABLE_METHOD_DELETE:
                this.handler.deleteTable(graph, partId, table);
                flag = true;
                break;
            case TABLE_METHOD_DROP:
                this.handler.dropTable(graph, partId, table);
                flag = true;
                break;
            case TABLE_METHOD_TRUNCATE:
                this.handler.truncate(graph, partId);
                flag = true;
                break;
            default:
                throw new UnsupportedOperationException("TableMethod: " + method.name());
        }

        return flag;
    }

    public boolean doGraph(int partId, GraphMethod method, String graph) {
        boolean flag = true;
        if (method == GRAPH_METHOD_DELETE) {// Hand over to raft for execution, no processing here
            flag = true;
        } else {
            throw new UnsupportedOperationException("GraphMethod: " + method.name());
        }
        return flag;
    }
}
