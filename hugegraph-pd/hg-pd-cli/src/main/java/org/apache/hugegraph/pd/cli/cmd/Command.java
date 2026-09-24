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

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;

import java.util.regex.Pattern;

public abstract class Command {

    protected static String error =
            "Startup parameters: command, pd address, command parameters, parameter separator " +
            "(optional)";
    protected PDClient pdClient;
    protected PDConfig config;

    public Command(String pd) {
        config = PDConfig.of(pd).setAuthority("store", "");
        pdClient = PDClient.create(config);
    }

    public static Parameter toParameter(String[] args) throws PDException {
        if (args.length < 3) {
            throw new PDException(-1, error);
        }
        Parameter parameter = new Parameter();
        parameter.setPd(args[0]);
        parameter.setCmd(args[1]);
        if (args.length == 3) {
            parameter.setParams(new String[]{args[2].trim()});
        } else {
            String t = args[3];
            if (t != null && !t.isEmpty()) {
                String[] raw = args[2].split(Pattern.quote(t));
                for (int i = 0; i < raw.length; i++) {
                    raw[i] = raw[i].trim();
                }
                parameter.setParams(raw);
                parameter.setSeparator(t);
            } else {
                parameter.setParams(new String[]{args[2].trim()});
            }
        }
        return parameter;
    }

    public abstract void action(String[] params) throws Exception;
}
