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

package org.apache.hugegraph.pd.meta;

import java.util.List;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;

public class LogMeta extends MetadataRocksDBStore {

    private PDConfig pdConfig;

    public LogMeta(PDConfig pdConfig) {
        super(pdConfig);
        this.pdConfig = pdConfig;
    }

    public void insertLog(Metapb.LogRecord record) throws PDException {
        byte[] storeLogKey = MetadataKeyHelper.getLogKey(record);
        put(storeLogKey, record.toByteArray());

    }

    public List<Metapb.LogRecord> getLog(String action, Long start, Long end) throws PDException {
        byte[] keyStart = MetadataKeyHelper.getLogKeyPrefix(action, start);
        byte[] keyEnd = MetadataKeyHelper.getLogKeyPrefix(action, end);
        List<Metapb.LogRecord> stores = this.scanRange(Metapb.LogRecord.parser(),
                                                       keyStart, keyEnd);
        return stores;
    }
}
