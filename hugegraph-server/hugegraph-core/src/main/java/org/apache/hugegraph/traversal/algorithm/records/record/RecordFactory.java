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

package org.apache.hugegraph.traversal.algorithm.records.record;

public class RecordFactory {

    public static Record newRecord(RecordType type) {
        return newRecord(type, false);
    }

    public static Record newRecord(RecordType type, boolean concurrent) {
        Record record = switch (type) {
            case INT -> new Int2IntRecord();
            case SET -> new Int2SetRecord();
            case ARRAY -> new Int2ArrayRecord();
            default -> throw new AssertionError("Unsupported record type: " + type);
        };

        if (concurrent && !record.concurrent()) {
            record = new SyncRecord(record);
        }

        return record;
    }
}
