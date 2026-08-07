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

package org.apache.hugegraph.unit.api.job;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.hugegraph.api.job.TaskResultPageTokenCodec;
import org.apache.hugegraph.api.job.TaskResultPageTokenCodec.Token;
import org.apache.hugegraph.task.TaskResultPageCursor.RootType;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class TaskResultPageTokenCodecTest {

    private static final String CURRENT_SECRET = secret("current-secret");
    private static final String PREVIOUS_SECRET = secret("previous-secret");

    @Test
    public void testRoundTrip() {
        TaskResultPageTokenCodec codec = codec("current", CURRENT_SECRET,
                                               null, null, 4096);
        Token expected = token();

        Token actual = codec.decode(codec.encode(expected), 150L);

        Assert.assertEquals("DEFAULT", actual.graphSpace());
        Assert.assertEquals("hugegraph", actual.graph());
        Assert.assertEquals(123L, actual.taskId());
        Assert.assertEquals(RootType.ARRAY, actual.rootType());
        Assert.assertEquals(42L, actual.nextOffset());
        Assert.assertEquals(20, actual.pageSize());
        Assert.assertEquals("fingerprint", actual.fingerprint());
        Assert.assertEquals(100L, actual.issuedAt());
        Assert.assertEquals(200L, actual.expiresAt());
    }

    @Test
    public void testRejectTamperedPayload() {
        TaskResultPageTokenCodec codec = codec("current", CURRENT_SECRET,
                                               null, null, 4096);
        String token = codec.encode(token());
        int payload = token.indexOf('.') + 2;
        char replacement = token.charAt(payload) == 'A' ? 'B' : 'A';
        String tampered = token.substring(0, payload) + replacement +
                          token.substring(payload + 1);

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> codec.decode(tampered, 150L));
    }

    @Test
    public void testRejectExpiredToken() {
        TaskResultPageTokenCodec codec = codec("current", CURRENT_SECRET,
                                               null, null, 4096);

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> codec.decode(codec.encode(token()), 201L));
    }

    @Test
    public void testRejectUnknownKey() {
        TaskResultPageTokenCodec issuer = codec("old", CURRENT_SECRET,
                                                null, null, 4096);
        TaskResultPageTokenCodec reader = codec("current", CURRENT_SECRET,
                                                null, null, 4096);

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> reader.decode(issuer.encode(token()), 150L));
    }

    @Test
    public void testRejectTokenOverLengthLimit() {
        TaskResultPageTokenCodec codec = codec("current", CURRENT_SECRET,
                                               null, null, 32);

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> codec.decode("x".repeat(33), 150L));
    }

    @Test
    public void testAcceptPreviousKeyDuringRotation() {
        TaskResultPageTokenCodec oldCodec = codec("old", PREVIOUS_SECRET,
                                                  null, null, 4096);
        TaskResultPageTokenCodec newCodec = codec("current", CURRENT_SECRET,
                                                  "old", PREVIOUS_SECRET,
                                                  4096);

        Token decoded = newCodec.decode(oldCodec.encode(token()), 150L);

        Assert.assertEquals(42L, decoded.nextOffset());
    }

    @Test
    public void testRejectDuplicateRotationKeyId() {
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> codec("current", CURRENT_SECRET,
                                        "current", PREVIOUS_SECRET, 4096));
    }

    @Test
    public void testRejectShortSecret() {
        String shortSecret = Base64.getUrlEncoder().withoutPadding()
                                   .encodeToString(new byte[31]);

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> codec("current", shortSecret,
                                        null, null, 4096));
    }

    private static Token token() {
        return new Token("DEFAULT", "hugegraph", 123L, RootType.ARRAY,
                         42L, 20, "fingerprint", 100L, 200L);
    }

    private static TaskResultPageTokenCodec codec(
            String currentKeyId, String currentSecret,
            String previousKeyId, String previousSecret, int maxLength) {
        return new TaskResultPageTokenCodec(currentKeyId, currentSecret,
                                            previousKeyId, previousSecret,
                                            maxLength);
    }

    private static String secret(String prefix) {
        String value = prefix + "-0123456789-0123456789-0123456789";
        return Base64.getUrlEncoder().withoutPadding().encodeToString(
                value.getBytes(StandardCharsets.UTF_8));
    }
}
