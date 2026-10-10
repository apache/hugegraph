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

package org.apache.hugegraph.store.grpc.query;

/**
 * Existing query responses carry either persistent rows or tagged graph elements.
 * Keep their selection aligned with the node's deserialization stage.
 */
public final class QueryResultFormat {

    private QueryResultFormat() {
    }

    public static boolean returnsTaggedElements(QueryRequest request) {
        return request.getScanType() == ScanType.NO_SCAN && request.getLoadPropertyFromIndex() ||
               requiresElementDeserialization(request);
    }

    public static boolean requiresElementDeserialization(QueryRequest request) {
        return request.getOrderByCount() > 0 || request.getPropertyCount() > 0 ||
               !request.getCondition().isEmpty() ||
               request.getFunctionsCount() > 0 && !request.getGroupBySchemaLabel();
    }
}
