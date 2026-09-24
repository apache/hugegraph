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

package org.apache.hugegraph.store.node.grpc.query.model;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.util.MultiKv;
import org.apache.hugegraph.structure.BaseElement;

import lombok.Data;

@Data
public class PipelineResult {

    public static final PipelineResult EMPTY = nullResult();

    private PipelineResultType resultType;
    private RocksDBSession.BackendColumn column;
    private BaseElement element;
    private MultiKv kv;
    private String message;

    public PipelineResult(RocksDBSession.BackendColumn column) {
        this.resultType = PipelineResultType.BACKEND_COLUMN;
        this.column = column;
    }

    public PipelineResult(BaseElement element) {
        this.resultType = PipelineResultType.HG_ELEMENT;
        this.element = element;
    }

    public PipelineResult(MultiKv kv) {
        this.resultType = PipelineResultType.MKV;
        this.kv = kv;
    }

    private PipelineResult() {
        this.resultType = PipelineResultType.NULL;
    }

    private PipelineResult(String message) {
        this.resultType = PipelineResultType.ERROR;
        this.message = message;
    }

    public static PipelineResult nullResult() {
        return new PipelineResult();
    }

    public static PipelineResult ofError(String message) {
        return new PipelineResult(message);
    }

    public boolean isEmpty() {
        return resultType == PipelineResultType.NULL;
    }

    public boolean isError() {
        return resultType == PipelineResultType.ERROR;
    }
}
