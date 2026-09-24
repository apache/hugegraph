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

package org.apache.hugegraph.store.cmd.request;

import lombok.Data;
import org.apache.hugegraph.store.cmd.HgCmdBase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class BatchPutRequest extends HgCmdBase.BaseRequest {

    private List<KV> entries = new ArrayList<>();

    @Override
    public byte magic() {
        return HgCmdBase.BATCH_PUT;
    }

    @Data
    public static class KV implements Serializable {

        private String table;
        private int code;
        private byte[] key;
        private byte[] value;

        public static KV of(String table, int code, byte[] key, byte[] value) {
            KV kv = new KV();
            kv.table = table;
            kv.code = code;
            kv.key = key;
            kv.value = value;
            return kv;
        }
    }
}
