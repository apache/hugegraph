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

package org.apache.hugegraph.unit.core;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hugegraph.testutil.Utils;
import org.junit.Assert;
import org.junit.Test;

public class ConfigPathTest {

    @Test
    public void testExplicitConfigAlsoAppliesToGraphCopies() throws Exception {
        String previous = System.getProperty("config_path");
        Path config = Files.createTempFile("hugegraph-config-path-", ".properties");
        try {
            Files.write(config, "hbase.port=22181\nstore=isolated\n"
                                .getBytes(StandardCharsets.UTF_8));
            System.setProperty("config_path", config.toString());
            Assert.assertEquals(22181, Utils.getConf().getInt("hbase.port"));
            Assert.assertEquals("isolated", Utils.getConf().getString("store"));
            Utils.getConf().setProperty("store", "copy");
            Assert.assertEquals("isolated", Utils.getConf().getString("store"));
        } finally {
            if (previous == null) {
                System.clearProperty("config_path");
            } else {
                System.setProperty("config_path", previous);
            }
            Files.deleteIfExists(config);
        }
    }
}
