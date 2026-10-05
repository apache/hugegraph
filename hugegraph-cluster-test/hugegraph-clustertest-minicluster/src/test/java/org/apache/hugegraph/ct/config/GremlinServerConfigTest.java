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

package org.apache.hugegraph.ct.config;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GremlinServerConfigTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testUpdateCommentedHostAndPort() throws Exception {
        File config = temporaryFolder.newFile("gremlin-server.yaml");
        String content = "#host: 127.0.0.1\n" +
                         "#port: 8182\n" +
                         "evaluationTimeout: 30000\n";
        Files.write(config.toPath(), content.getBytes(StandardCharsets.UTF_8));

        GremlinServerConfig.update(config.toPath(), "127.0.0.1", 12345);

        List<String> updated = Files.readAllLines(config.toPath(),
                                                  StandardCharsets.UTF_8);
        Assert.assertTrue(updated.toString(),
                          updated.contains("host: 127.0.0.1"));
        Assert.assertTrue(updated.toString(), updated.contains("port: 12345"));
        Assert.assertFalse(updated.toString(), updated.contains("port: 8182"));
    }
}
