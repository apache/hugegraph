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

package org.apache.hugegraph.pd;

import java.util.List;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.LogMeta;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.springframework.stereotype.Service;

import com.google.protobuf.Any;
import com.google.protobuf.GeneratedMessageV3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class LogService {

    public static final String GRPC = "GRPC";
    public static final String REST = "REST";
    public static final String TASK = "TASK";
    public static final String NODE_CHANGE = "NODE_CHANGE";
    public static final String PARTITION_CHANGE = "PARTITION_CHANGE";
    private LogMeta logMeta;

    public LogService(PDConfig pdConfig) {
        logMeta = MetadataFactory.newLogMeta(pdConfig);
    }

    public List<Metapb.LogRecord> getLog(String action, Long start, Long end) throws PDException {
        return logMeta.getLog(action, start, end);
    }

    public void insertLog(String action, String message, GeneratedMessageV3 target) {
        try {
            Metapb.LogRecord logRecord = Metapb.LogRecord.newBuilder()
                                                         .setAction(action)
                                                         .setMessage(message)
                                                         .setTimestamp(System.currentTimeMillis())
                                                         .setObject(Any.pack(target))
                                                         .build();
            logMeta.insertLog(logRecord);
        } catch (PDException e) {
            log.debug("Insert log with error:{}", e);
        }

    }
}
