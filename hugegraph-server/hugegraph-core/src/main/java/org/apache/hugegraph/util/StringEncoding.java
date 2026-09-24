// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.hugegraph.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.UUID;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.mindrot.jbcrypt.BCrypt;

import com.google.common.base.CharMatcher;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author HugeGraph Authors
 */
public final class StringEncoding {

    private static final MessageDigest DIGEST;
    private static final byte[] BYTES_EMPTY = new byte[0];
    private static final String STRING_EMPTY = "";
    private static final int BLOCK_SIZE = 4096;
    private static final int BCRYPT_WORK_FACTOR = 10;

    static {
        final String ALG = "SHA-256";
        try {
            DIGEST = MessageDigest.getInstance(ALG);
        } catch (NoSuchAlgorithmException e) {
            throw new HugeException("Failed to load algorithm %s", e, ALG);
        }
    }

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

    /** Similar to {@link StringSerializer} */
    public static int writeAsciiString(byte[] array, int offset, String value) {
        E.checkArgument(CharMatcher.ascii().matchesAllOf(value),
                        "'%s' must be ASCII string", value);
        int len = value.length();
        if (len == 0) {
            array[offset++] = (byte) 0x80;
            return offset;
        }

        int i = 0;
        do {
            int c = value.charAt(i);
            assert c <= 127;
            byte b = (byte) c;
            if (++i == len) {
                // End marker
                b |= 0x80;
            }
            array[offset++] = b;
        } while (i < len);

        return offset;
    }

    public static String readAsciiString(byte[] array, int offset) {
        StringBuilder sb = new StringBuilder();
        int c;
        do {
            c = 0xFF & array[offset++];
            if (c != 0x80) {
                sb.append((char) (c & 0x7F));
            }
        } while ((c & 0x80) <= 0);
        return sb.toString();
    }

    public static int getAsciiByteLength(String value) {
        E.checkArgument(CharMatcher.ascii().matchesAllOf(value),
                        "'%s' must be ASCII string", value);
        return value.isEmpty() ? 1 : value.length();
    }

    public static byte[] encode(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String decode(byte[] bytes) {
        if (bytes.length == 0) {
            return STRING_EMPTY;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static String decode(byte[] bytes, int offset, int length) {
        if (length == 0) {
            return STRING_EMPTY;
        }
        return new String(bytes, offset, length, StandardCharsets.UTF_8);
    }

    public static String encodeBase64(byte[] bytes) {
        return BASE64_ENCODER.encodeToString(bytes);
    }

    public static byte[] decodeBase64(String value) {
        if (value.isEmpty()) {
            return BYTES_EMPTY;
        }
        return BASE64_DECODER.decode(value);
    }

    public static byte[] compress(String value) {
        return compress(value, LZ4Util.DEFAULT_BUFFER_RATIO);
    }

    public static byte[] compress(String value, float bufferRatio) {
        BytesBuffer buf = LZ4Util.compress(encode(value), BLOCK_SIZE, bufferRatio);
        return buf.bytes();
    }

    public static String decompress(byte[] value) {
        return decompress(value, LZ4Util.DEFAULT_BUFFER_RATIO);
    }

    public static String decompress(byte[] value, float bufferRatio) {
        BytesBuffer buf = LZ4Util.decompress(value, BLOCK_SIZE, bufferRatio);
        return decode(buf.array(), 0, buf.position());
    }

    public static String hashPassword(String password) {
        // OWASP suggests 10 as a minimum and 12–14 for production;
        // workFactor 12 is not used by default due to its 200+ ms cost.
        return BCrypt.hashpw(password, BCrypt.gensalt(BCRYPT_WORK_FACTOR));
    }

    public static boolean checkPassword(String candidatePassword, String dbPassword) {
        return BCrypt.checkpw(candidatePassword, dbPassword);
    }

    public static String sha256(String string) {
        byte[] stringBytes = encode(string);
        DIGEST.reset();
        return StringEncoding.encodeBase64(DIGEST.digest(stringBytes));
    }

    public static String format(byte[] bytes) {
        return String.format("%s[0x%s]", decode(bytes), Bytes.toHex(bytes));
    }

    public static UUID uuid(String value) {
        E.checkArgument(value != null, "The UUID can't be null");
        try {
            if (value.contains("-") && value.length() == 36) {
                return UUID.fromString(value);
            }
            // UUID represented by hex string
            E.checkArgument(value.length() == 32, "Invalid UUID string: %s", value);
            String high = value.substring(0, 16);
            String low = value.substring(16);
            return new UUID(Long.parseUnsignedLong(high, 16),
                            Long.parseUnsignedLong(low, 16));
        } catch (NumberFormatException ignored) {
            throw new IllegalArgumentException("Invalid UUID string: " + value);
        }
    }
}
