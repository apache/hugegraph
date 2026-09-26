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

package org.apache.hugegraph.SimpleClusterTest;

import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class SimpleClusterFileTest extends BaseSimpleTest {

    @Test
    public void checkPDNodeDir() {
        for (String nodeDir : env.getPDNodeDir()) {
            Assert.assertTrue(new File(nodeDir).isDirectory());
        }
    }

    @Test
    public void checkStoreNodeDir() {
        for (String nodeDir : env.getStoreNodeDir()) {
            Assert.assertTrue(new File(nodeDir).isDirectory());
        }
    }

    @Test
    public void checkServerNodeDir() {
        for (String nodeDir : env.getServerNodeDir()) {
            Assert.assertTrue(new File(nodeDir).isDirectory());
        }
    }

    @Test
    public void checkServerMetadataPDPeers() throws Exception {
        String expected = String.join(",", env.getPDGrpcAddrs());
        for (String nodeDir : env.getServerNodeDir()) {
            Properties config = new Properties();
            try (Reader reader = Files.newBufferedReader(
                    Paths.get(nodeDir, "conf", "rest-server.properties"))) {
                config.load(reader);
            }
            Assert.assertEquals(expected, config.getProperty("pd.peers"));
        }
    }

}
