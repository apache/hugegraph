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

package org.apache.hugegraph.pd.cli;

import org.apache.hugegraph.pd.cli.cmd.ChangeRaft;
import org.apache.hugegraph.pd.cli.cmd.CheckPeers;
import org.apache.hugegraph.pd.cli.cmd.Command;
import org.apache.hugegraph.pd.cli.cmd.Config;
import org.apache.hugegraph.pd.cli.cmd.Parameter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CliApplication {

    public static void main(String[] args) {
        try {
            Parameter parameter = Command.toParameter(args);
            Command command;
            switch (parameter.getCmd()) {
                case "config":
                    command = new Config(parameter.getPd());
                    break;
                case "change_raft":
                    command = new ChangeRaft(parameter.getPd());
                    break;
                case "check_peers":
                    command = new CheckPeers(parameter.getPd());
                    break;
                default:
                    log.error("Invalid command: {}. Supported: config|change_raft|check_peers",
                              parameter.getCmd());
                    return;
            }
            command.action(parameter.getParams());
        } catch (Exception e) {
            log.error("main thread error:", e);
            System.exit(1);
        } finally {

        }

    }
}
