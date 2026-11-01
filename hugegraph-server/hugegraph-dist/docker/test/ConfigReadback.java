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
 *
 */

import org.apache.hugegraph.config.HugeConfig;

public final class ConfigReadback {

    public static void main(String[] args) {
        HugeConfig config = new HugeConfig(args[0]);
        Object actual = config.getProperty("server.service_id");
        if (!args[1].equals(actual)) {
            throw new AssertionError("Expected service ID [" + args[1] + "] but loaded [" + actual + "]");
        }
        System.out.println("PASS: HugeConfig service ID readback [" + actual + "]");
    }
}
