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

package org.apache.hugegraph.pd.cli.cmd;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;

public class Config extends Command {

    public Config(String pd) {
        super(pd);
    }

    @Override
    public void action(String[] params) throws PDException {
        if (params == null || params.length < 1 || params[0] == null ||
            params[0].trim().isEmpty()) {
            System.err.println("Usage: config <key>[=<value>] (keys: shardCount, enableBatchLoad)");
            return;
        }
        String param = params[0].trim();
        String[] pair = param.split("=");
        String key = pair[0].trim();
        Object value = null;
        if (pair.length > 1) {
            value = pair[1].trim();
        }
        if (value == null) {
            Metapb.PDConfig pdConfig = pdClient.getPDConfig();
            switch (key) {
                case "enableBatchLoad":
                    //    value = pdConfig.getEnableBatchLoad();
                    break;
                case "shardCount":
                    value = pdConfig.getShardCount();
                    break;
            }

            System.out.println("Get config " + key + "=" + value);
        } else {
            Metapb.PDConfig.Builder builder = Metapb.PDConfig.newBuilder();
            boolean changed = false;
            switch (key) {
                case "enableBatchLoad":
                    //   builder.setEnableBatchLoad(Boolean.valueOf((String)value));
                    break;
                case "shardCount":
                    try {
                        builder.setShardCount(Integer.valueOf((String) value));
                        changed = true;
                    } catch (NumberFormatException nfe) {
                        System.err.println("Invalid integer for shardCount: " + value);
                        return;
                    }
                    break;
                default:
                    System.err.println(
                            "Unknown key: " + key + " (supported: shardCount, enableBatchLoad)");
                    return;
            }
            if (changed) {
                pdClient.setPDConfig(builder.build());
                System.out.println("Set config " + key + "=" + value);
            } else {
                System.err.println("No change applied");
            }
        }
    }
}
