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

package org.apache.hugegraph.api.job;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.task.TaskResultPageCursor.RootType;
import org.apache.hugegraph.util.E;

public final class TaskResultPageTokenCodec {

    private static final byte VERSION = 1;
    private static final int MIN_SECRET_BYTES = 32;
    private static final String HMAC_ALGORITHM = "HmacSHA256";

    private final Key currentKey;
    private final Key previousKey;
    private final int maxTokenLength;

    public TaskResultPageTokenCodec(String currentKeyId, String currentSecret,
                                    String previousKeyId,
                                    String previousSecret,
                                    int maxTokenLength) {
        this.currentKey = new Key(currentKeyId, currentSecret);
        this.previousKey = optionalKey(previousKeyId, previousSecret);
        E.checkArgument(this.previousKey == null ||
                        !this.currentKey.id.equals(this.previousKey.id),
                        "The current and previous page token key ids " +
                        "must differ");
        E.checkArgument(maxTokenLength > 0,
                        "The maximum token length must be positive");
        this.maxTokenLength = maxTokenLength;
    }

    public static TaskResultPageTokenCodec fromConfig(HugeConfig config) {
        E.checkNotNull(config, "server config");
        return new TaskResultPageTokenCodec(
                config.get(ServerOptions.TASK_RESULT_PAGE_TOKEN_KEY_ID),
                config.get(ServerOptions.TASK_RESULT_PAGE_TOKEN_SECRET),
                config.get(
                        ServerOptions.TASK_RESULT_PAGE_TOKEN_PREVIOUS_KEY_ID),
                config.get(
                        ServerOptions.TASK_RESULT_PAGE_TOKEN_PREVIOUS_SECRET),
                config.get(
                        ServerOptions.TASK_RESULT_PAGE_TOKEN_LENGTH_MAX));
    }

    public String encode(Token token) {
        E.checkNotNull(token, "page token");
        byte[] payload = encodePayload(token);
        String payloadText = encodeBase64(payload);
        String signed = this.currentKey.id + "." + payloadText;
        String value = signed + "." +
                       encodeBase64(sign(this.currentKey.secret, signed));
        E.checkArgument(value.length() <= this.maxTokenLength,
                        "The encoded page token exceeds the length limit");
        return value;
    }

    public Token decode(String value, long nowSeconds) {
        E.checkArgument(value != null && !value.isEmpty(),
                        "The page token can't be empty");
        E.checkArgument(value.length() <= this.maxTokenLength,
                        "The page token exceeds the length limit");
        String[] parts = value.split("\\.", -1);
        E.checkArgument(parts.length == 3 && !parts[0].isEmpty() &&
                        !parts[1].isEmpty() && !parts[2].isEmpty(),
                        "The page token is malformed");

        Key key = this.key(parts[0]);
        String signed = parts[0] + "." + parts[1];
        byte[] actualMac = decodeBase64(parts[2]);
        byte[] expectedMac = sign(key.secret, signed);
        E.checkArgument(MessageDigest.isEqual(expectedMac, actualMac),
                        "The page token signature is invalid");

        Token token = decodePayload(decodeBase64(parts[1]));
        E.checkArgument(token.issuedAt <= token.expiresAt,
                        "The page token time range is invalid");
        E.checkArgument(nowSeconds <= token.expiresAt,
                        "The page token has expired");
        return token;
    }

    private Key key(String id) {
        if (this.currentKey.id.equals(id)) {
            return this.currentKey;
        }
        if (this.previousKey != null && this.previousKey.id.equals(id)) {
            return this.previousKey;
        }
        throw new IllegalArgumentException("The page token key is unknown");
    }

    private static byte[] encodePayload(Token token) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(VERSION);
            output.writeUTF(token.graphSpace);
            output.writeUTF(token.graph);
            output.writeLong(token.taskId);
            output.writeByte(token.rootType.ordinal());
            output.writeLong(token.nextOffset);
            output.writeInt(token.pageSize);
            output.writeUTF(token.fingerprint);
            output.writeLong(token.issuedAt);
            output.writeLong(token.expiresAt);
            output.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new HugeException("Failed to encode page token", e);
        }
    }

    private static Token decodePayload(byte[] payload) {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(payload);
             DataInputStream input = new DataInputStream(bytes)) {
            int version = input.readUnsignedByte();
            E.checkArgument(version == VERSION,
                            "The page token version is unsupported");
            String graphSpace = input.readUTF();
            String graph = input.readUTF();
            long taskId = input.readLong();
            int rootOrdinal = input.readUnsignedByte();
            E.checkArgument(rootOrdinal < RootType.values().length,
                            "The page token root type is invalid");
            long nextOffset = input.readLong();
            int pageSize = input.readInt();
            String fingerprint = input.readUTF();
            long issuedAt = input.readLong();
            long expiresAt = input.readLong();
            E.checkArgument(bytes.available() == 0,
                            "The page token has trailing data");
            return new Token(graphSpace, graph, taskId,
                             RootType.values()[rootOrdinal], nextOffset,
                             pageSize, fingerprint, issuedAt, expiresAt);
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException("The page token is malformed",
                                               e);
        }
    }

    private static byte[] sign(byte[] secret, String value) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(secret, HMAC_ALGORITHM));
            return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new HugeException("Failed to sign page token", e);
        }
    }

    private static Key optionalKey(String id, String secret) {
        boolean idEmpty = id == null || id.isEmpty();
        boolean secretEmpty = secret == null || secret.isEmpty();
        E.checkArgument(idEmpty == secretEmpty,
                        "The previous page token key id and secret must " +
                        "be configured together");
        return idEmpty ? null : new Key(id, secret);
    }

    private static String encodeBase64(byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    private static byte[] decodeBase64(String value) {
        try {
            return Base64.getUrlDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("The page token is malformed",
                                               e);
        }
    }

    private static final class Key {

        private final String id;
        private final byte[] secret;

        private Key(String id, String secret) {
            E.checkArgument(id != null && !id.isEmpty() &&
                            id.indexOf('.') < 0,
                            "The page token key id is invalid");
            this.id = id;
            this.secret = decodeBase64(secret);
            E.checkArgument(this.secret.length >= MIN_SECRET_BYTES,
                            "The page token secret must contain at least " +
                            "%s bytes", MIN_SECRET_BYTES);
        }
    }

    public static final class Token {

        private final String graphSpace;
        private final String graph;
        private final long taskId;
        private final RootType rootType;
        private final long nextOffset;
        private final int pageSize;
        private final String fingerprint;
        private final long issuedAt;
        private final long expiresAt;

        public Token(String graphSpace, String graph, long taskId,
                     RootType rootType, long nextOffset, int pageSize,
                     String fingerprint, long issuedAt, long expiresAt) {
            E.checkArgument(graphSpace != null && !graphSpace.isEmpty(),
                            "The graphspace can't be empty");
            E.checkArgument(graph != null && !graph.isEmpty(),
                            "The graph can't be empty");
            E.checkNotNull(rootType, "root type");
            E.checkArgument(nextOffset >= 0L,
                            "The next offset must be non-negative");
            E.checkArgument(pageSize > 0,
                            "The page size must be positive");
            E.checkArgument(fingerprint != null && !fingerprint.isEmpty(),
                            "The result fingerprint can't be empty");
            this.graphSpace = graphSpace;
            this.graph = graph;
            this.taskId = taskId;
            this.rootType = rootType;
            this.nextOffset = nextOffset;
            this.pageSize = pageSize;
            this.fingerprint = fingerprint;
            this.issuedAt = issuedAt;
            this.expiresAt = expiresAt;
        }

        public String graphSpace() {
            return this.graphSpace;
        }

        public String graph() {
            return this.graph;
        }

        public long taskId() {
            return this.taskId;
        }

        public RootType rootType() {
            return this.rootType;
        }

        public long nextOffset() {
            return this.nextOffset;
        }

        public int pageSize() {
            return this.pageSize;
        }

        public String fingerprint() {
            return this.fingerprint;
        }

        public long issuedAt() {
            return this.issuedAt;
        }

        public long expiresAt() {
            return this.expiresAt;
        }
    }
}
