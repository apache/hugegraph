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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.apache.hugegraph.store.grpc.stream.ScanCondition;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.node.util.HgAssert;

import lombok.extern.slf4j.Slf4j;

/**
 * Buffering the data of ScanQueryRequest and generating ScanQuery.
 * It will not hold the reference of ScanQueryRequest.
 * <p>
 * 2023/2/8
 */
@NotThreadSafe
@Slf4j
class ScanQueryProducer implements Iterable<ScanQuery> {

    private String graph;
    private String[] tables;
    private ScanMethod method;
    private int scanType;
    private byte[] query;
    private byte[] position;

    private List<ScanCondition> conditionList;

    private ScanQueryProducer() {
    }

    public static ScanQueryProducer requestOf(String graph, String[] tables,
                                              ScanQueryRequest request) {
        HgAssert.isArgumentValid(graph, "graph");
        HgAssert.isArgumentNotNull(tables, "tables");
        HgAssert.isArgumentNotNull(request, "ScanQueryRequest");
        HgAssert.isFalse(request.getOrderType() == ScanOrderType.ORDER_BY_KEY,
                         "ORDER_BY_KEY is not supported by batch scan");

        ScanQueryProducer res = new ScanQueryProducer();
        res.graph = graph;
        res.tables = tables; // a trick that reduce the data-size transferred through network;

        res.method = request.getMethod();
        res.scanType = request.getScanType();
        res.query = request.getQuery().toByteArray();
        res.position = request.getPosition().toByteArray();

        res.conditionList = request.getConditionList();

        if (res.conditionList == null) {
            res.conditionList = Collections.emptyList();
        }

        if (res.conditionList.isEmpty()) {
            log.warn("the condition-list of ScanQueryRequest is empty.");
        }

        return res;
    }

    private ScanQuery createQuery(String tableName, ScanCondition condition) {
        ScanQuery sq = ScanQuery.of();
        sq.graph = this.graph;
        sq.table = tableName;
        sq.method = this.method;
        sq.scanType = this.scanType;
        sq.query = this.query;
        sq.position = this.position;

        if (condition != null) {
            sq.keyCode = condition.getCode();
            sq.start = condition.getStart().toByteArray();
            sq.end = condition.getEnd().toByteArray();
            sq.prefix = condition.getPrefix().toByteArray();
            sq.serialNo = condition.getSerialNo();
        }

        return sq;
    }

    private String getTableName(int tableIndex) {
        if (tableIndex + 1 > this.tables.length) {
            return null;
        }

        return this.tables[tableIndex];
    }

    @Override
    public Iterator<ScanQuery> iterator() {
        if (this.conditionList.isEmpty()) {
            return new NoConditionsIterator();
        } else {
            return new ConditionsIterator();
        }
    }

    /**
     * Return an Iterator contains Scan-Queries grouped ScanQuery that
     * created by same resource but filled with different tables;
     *
     * @return
     */
    public Iterator<ScanQuery[]> groupedIterator() {
        if (this.conditionList.isEmpty()) {
            return new GroupedNoConditionsIterator();
        } else {
            return new GroupedConditionsIterator();
        }
    }

    /*---------------inner classes below--------------------*/

    private class GroupedNoConditionsIterator implements Iterator<ScanQuery[]> {

        private boolean isHasNext = true;

        @Override
        public boolean hasNext() {
            return isHasNext;
        }

        @Override
        public ScanQuery[] next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            ScanQuery[] res = new ScanQuery[ScanQueryProducer.this.tables.length];

            for (int i = 0; i < res.length; i++) {
                res[i] = ScanQueryProducer.this.createQuery(ScanQueryProducer.this.tables[i], null);
            }

            this.isHasNext = false;

            return res;
        }
    }

    private class GroupedConditionsIterator implements Iterator<ScanQuery[]> {

        private final Iterator<ScanCondition> conditionIterator =
                ScanQueryProducer.this.conditionList.iterator();

        @Override
        public boolean hasNext() {
            return conditionIterator.hasNext();
        }

        @Override
        public ScanQuery[] next() {
            ScanCondition condition = this.conditionIterator.next();
            ScanQuery[] res = new ScanQuery[ScanQueryProducer.this.tables.length];

            for (int i = 0; i < res.length; i++) {
                res[i] = ScanQueryProducer.this.createQuery(ScanQueryProducer.this.tables[i],
                                                            condition);
            }

            return res;
        }
    }

    /**
     * TODO: no testing
     */
    private class NoConditionsIterator implements Iterator<ScanQuery> {

        private String tableName;
        private int tableIndex;

        @Override
        public boolean hasNext() {
            if (this.tableName != null) {
                return true;
            }

            this.tableName = ScanQueryProducer.this.getTableName(this.tableIndex);

            return this.tableName != null;
        }

        @Override
        public ScanQuery next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            ScanQuery res = ScanQueryProducer.this.createQuery(this.tableName, null);
            this.tableIndex++;
            this.tableName = ScanQueryProducer.this.getTableName(this.tableIndex);

            return res;
        }

    }

    /**
     * TODO: no testing
     */
    private class ConditionsIterator implements Iterator<ScanQuery> {

        private final Iterator<ScanCondition> conditionIterator =
                ScanQueryProducer.this.conditionList.iterator();
        private ScanCondition condition;
        private String tableName;
        private int tableIndex;

        @Override
        public boolean hasNext() {
            if (this.condition != null) {
                return true;
            }
            return conditionIterator.hasNext();
        }

        @Override
        public ScanQuery next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            if (this.condition == null) {
                this.condition = conditionIterator.next();
            }
            if (this.tableName == null) {
                this.tableName = ScanQueryProducer.this.getTableName(this.tableIndex);
            }

            ScanQuery res = ScanQueryProducer.this.createQuery(this.tableName, this.condition);
            this.tableIndex++;
            this.tableName = ScanQueryProducer.this.getTableName(this.tableIndex);

            if (this.tableName == null) {
                this.condition = null;
                this.tableIndex = 0;
            }

            return res;
        }

    }
}
