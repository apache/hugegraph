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

public class ChangeRaft extends Command {

    public ChangeRaft(String pd) {
        super(pd);
    }

    @Override
    public void action(String[] params) throws PDException {
        if (params == null || params.length < 1 || params[0] == null ||
            params[0].trim().isEmpty()) {
            System.err.println("Usage: change_raft <new_peers>");
            return;
        }
        pdClient.updatePdRaft(params[0]);
    }
}
