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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.task.TaskResultSnapshot;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.StringEncoding;
import org.junit.Test;

public class TaskResultSnapshotTest {

    @Test
    public void testOpenResultStreamMoreThanOnce() throws Exception {
        String json = "[{\"name\":\"marko\"},{\"name\":\"vadas\"}]";
        byte[] compressed = StringEncoding.compress(json);
        TaskResultSnapshot snapshot = new TaskResultSnapshot(
                IdGenerator.of(123L), TaskStatus.SUCCESS, compressed);

        Assert.assertEquals(IdGenerator.of(123L), snapshot.taskId());
        Assert.assertEquals(TaskStatus.SUCCESS, snapshot.status());
        Assert.assertTrue(snapshot.hasResult());
        Assert.assertEquals(json, read(snapshot.openResultStream()));
        Assert.assertEquals(json, read(snapshot.openResultStream()));

        byte[] digest = MessageDigest.getInstance("SHA-256")
                                     .digest(compressed);
        String expected = Base64.getUrlEncoder().withoutPadding()
                                .encodeToString(digest);
        Assert.assertEquals(expected, snapshot.fingerprint());
        Assert.assertEquals(expected, snapshot.fingerprint());
    }

    @Test
    public void testRejectOpeningMissingResult() {
        TaskResultSnapshot snapshot = new TaskResultSnapshot(
                IdGenerator.of(123L), TaskStatus.SUCCESS, null);

        Assert.assertFalse(snapshot.hasResult());
        Assert.assertThrows(IllegalStateException.class,
                            snapshot::openResultStream);
    }

    private static String read(InputStream input) throws IOException {
        try (InputStream stream = input;
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[64];
            int length;
            while ((length = stream.read(buffer)) != -1) {
                output.write(buffer, 0, length);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
