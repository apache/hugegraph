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

package org.apache.hugegraph.store.cli;

import org.apache.hugegraph.pd.cli.cmd.ChangeRaft;
import org.apache.hugegraph.pd.cli.cmd.CheckPeers;
import org.apache.hugegraph.pd.cli.cmd.Command;
import org.apache.hugegraph.pd.cli.cmd.Parameter;
import org.apache.hugegraph.store.cli.cmd.Load;
import org.apache.hugegraph.store.cli.cmd.MultiQuery;
import org.apache.hugegraph.store.cli.cmd.ScanShard;
import org.apache.hugegraph.store.cli.cmd.ScanSingleShard;
import org.apache.hugegraph.store.cli.cmd.ScanTable;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.extern.slf4j.Slf4j;

/**
 * 2022/2/14
 */
@SpringBootApplication
@Slf4j
public class CliApplication {

    public static void main(String[] args) {
        Parameter parameter;
        try {
            parameter = Command.toParameter(args);
            Command command;
            switch (parameter.getCmd()) {
                case "load":
                    command = new Load(parameter.getPd());
                    break;
                case "change_raft":
                    command = new ChangeRaft(parameter.getPd());
                    break;
                case "check_peers":
                    command = new CheckPeers(parameter.getPd());
                    break;
                case "query":
                    command = new MultiQuery(parameter.getPd());
                    break;
                case "scan":
                    command = new ScanTable(parameter.getPd());
                    break;
                case "shard":
                    command = new ScanShard(parameter.getPd());
                    break;
                case "shard-single":
                    command = new ScanSingleShard(parameter.getPd());
                    break;
                default:
                    log.error("Parameter err, no program executed");
                    return;
            }
            command.action(parameter.getParams());
        } catch (Exception e) {
            log.error("run cli command with error:", e);
        }
        System.exit(0);

    }
}
