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

package org.apache.hugegraph.store.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.Test;

public class ToplingProfileConfigTest {

    @Test
    public void testJavaWriteBatchProfileDisablesMmapWalIndex() throws IOException {
        assertMemtableAsLogIndexFalse(repoFile(
                "hugegraph-store/hg-store-dist/src/assembly/static/conf/rocksdb_store.yaml"));
        assertMemtableAsLogIndexFalse(repoFile(
                "hugegraph-pd/hg-pd-dist/src/assembly/static/conf/rocksdb_pd.yaml"));
        assertMemtableAsLogIndexFalse(repoFile(
                "hugegraph-server/hugegraph-dist/src/assembly/static/conf/toplingdb.yaml"));
    }

    private static void assertMemtableAsLogIndexFalse(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file);
        int matches = 0;
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.startsWith("#") || !trimmed.startsWith("memtable_as_log_index:")) {
                continue;
            }
            matches++;
            assertEquals(file.toString(), "false",
                         trimmed.substring("memtable_as_log_index:".length()).trim());
        }
        assertTrue(file.toString(), matches == 1);
    }

    private static Path repoFile(String relative) {
        Path module = Path.of("").toAbsolutePath();
        Path repo = module.getParent().getParent();
        Path file = repo.resolve(relative);
        assertTrue(file.toString(), Files.isRegularFile(file));
        return file;
    }
}
