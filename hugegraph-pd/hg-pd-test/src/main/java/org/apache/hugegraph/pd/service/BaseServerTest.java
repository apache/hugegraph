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

package org.apache.hugegraph.pd.service;

import java.io.File;
import java.net.http.HttpClient;
import java.time.Duration;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.BaseTest;
import org.apache.hugegraph.pd.config.PDConfig;
import org.junit.After;
import org.junit.BeforeClass;

public class BaseServerTest extends BaseTest {

    public static HttpClient client;

    @BeforeClass
    public static void init() {
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public static PDConfig getConfig() {
        FileUtils.deleteQuietly(new File("tmp/test/"));
        PDConfig pdConfig = new PDConfig() {{
            this.setClusterId(100);
            this.setPatrolInterval(1);
            this.setRaft(new Raft() {{
                setEnable(false);
            }});
            this.setDataPath("tmp/test/");
        }};
        return pdConfig;
    }

    @After
    public void teardown() {
        // pass
    }

}
