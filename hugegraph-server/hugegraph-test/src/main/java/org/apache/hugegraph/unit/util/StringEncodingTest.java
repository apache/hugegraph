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

package org.apache.hugegraph.unit.util;

import java.util.UUID;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.StringEncoding;
import org.junit.Test;
import org.mindrot.jbcrypt.BCrypt;

public class StringEncodingTest {

    @Test
    public void testEncode() {
        Assert.assertArrayEquals(new byte[]{},
                                 StringEncoding.encode(""));
        Assert.assertArrayEquals(new byte[]{97, 98, 99},
                                 StringEncoding.encode("abc"));
    }

    @Test
    public void testDecode() {
        Assert.assertEquals("abc",
                            StringEncoding.decode(new byte[]{97, 98, 99}));
        Assert.assertEquals("", StringEncoding.decode(new byte[]{}));
    }

    @Test
    public void testEncodeBase64() {
        byte[] bytes = new byte[]{97, 49, 50, 51, 52};
        Assert.assertEquals("YTEyMzQ=",
                            StringEncoding.encodeBase64(bytes));
        bytes = new byte[]{};
        Assert.assertEquals("",
                            StringEncoding.encodeBase64(bytes));
    }

    @Test
    public void testDecodeBase64() {
        byte[] bytes = new byte[]{97, 49, 50, 51, 52};
        Assert.assertArrayEquals(bytes,
                                 StringEncoding.decodeBase64("YTEyMzQ="));
        Assert.assertArrayEquals(new byte[]{},
                                 StringEncoding.decodeBase64(""));
    }

    @Test
    public void testPassword() {
        String passwd = StringEncoding.hashPassword("");
        String passwd2 = StringEncoding.hashPassword("");
        Assert.assertNotEquals(passwd, passwd2);
        Assert.assertTrue(StringEncoding.checkPassword("", passwd));
        Assert.assertTrue(StringEncoding.checkPassword("", passwd2));

        passwd = StringEncoding.hashPassword("123");
        passwd2 = StringEncoding.hashPassword("123");
        Assert.assertNotEquals(passwd, passwd2);
        Assert.assertTrue(StringEncoding.checkPassword("123", passwd));
        Assert.assertTrue(StringEncoding.checkPassword("123", passwd2));

        passwd = StringEncoding.hashPassword("123456");
        passwd2 = StringEncoding.hashPassword("123456");
        Assert.assertNotEquals(passwd, passwd2);
        Assert.assertTrue(StringEncoding.checkPassword("123456", passwd));
        Assert.assertTrue(StringEncoding.checkPassword("123456", passwd2));
    }

    @Test
    public void testSha256() {
        Assert.assertEquals("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
                            StringEncoding.sha256(""));
        Assert.assertEquals("pmWkWSBCL51Bfkhn79xPuKBKHz//H6B+mY6G9/eieuM=",
                            StringEncoding.sha256("123"));
        Assert.assertEquals("jZae727K08KaOmKSgOaGzww/XVqGr/PKEgIMkjrcbJI=",
                            StringEncoding.sha256("123456"));
    }

    @Test
    public void testUuid() {
        UUID uuid = UUID.fromString("835e1153-9281-4957-8691-cf79258e90eb");
        Assert.assertEquals(uuid, StringEncoding.uuid(
                "835e1153-9281-4957-8691-cf79258e90eb"));
        Assert.assertEquals(uuid, StringEncoding.uuid(
                "835e1153928149578691cf79258e90eb"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringEncoding.uuid("");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringEncoding.uuid("123");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringEncoding.uuid("835e1153-9281-4957-8691cf79258e90eb");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringEncoding.uuid("835e1153-9281-4957-8691-cf79258e90eg");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringEncoding.uuid("835e1153-9281-4957-8691-cf79258e90ebc");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringEncoding.uuid("835e1153928149578691cf79258e90ebc");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringEncoding.uuid("835e1153928149578691cf79258e90eg");
        });
    }

    @Test
    public void testFormat() {
        Assert.assertEquals("abc[0x616263]",
                            StringEncoding.format(new byte[]{97, 98, 99}));
        Assert.assertTrue(StringEncoding.format(new byte[]{1, -1, 0})
                                        .endsWith("\u0000[0x01ff00]"));
        Assert.assertEquals("", StringEncoding.decode(new byte[]{}));
    }

    @Test
    public void testCompress() {
        Assert.assertArrayEquals(Bytes.fromHex("4c5a34426c6f636b12000" +
                                               "000000000000000000000"),
                                 StringEncoding.compress(""));

        Assert.assertArrayEquals(Bytes.fromHex("4c5a34426c6f636b12030" +
                                               "000000300000022b24c0d" +
                                               "6162634c5a34426c6f636" +
                                               "b12000000000000000000000000"),
                                 StringEncoding.compress("abc"));
    }

    @Test
    public void testDecompress() {
        Assert.assertEquals("", StringEncoding.decompress(Bytes.fromHex(
                "4c5a34426c6f636b12000000000000000000000000")));
        Assert.assertEquals("abc", StringEncoding.decompress(Bytes.fromHex(
                "4c5a34426c6f636b12030000000300000022b24c0d616" +
                "2634c5a34426c6f636b12000000000000000000000000")));
    }

    @Test
    public void testWriteAsciiString() {
        String str = "abc133";
        int length = StringEncoding.getAsciiByteLength(str);
        Assert.assertEquals(6, length);
        byte[] buf = new byte[length];
        Assert.assertEquals(length,
                            StringEncoding.writeAsciiString(buf, 0, str));
        Assert.assertArrayEquals(Bytes.fromHex("6162633133b3"), buf);

        str = "";
        length = StringEncoding.getAsciiByteLength(str);
        Assert.assertEquals(1, length);
        buf = new byte[length];
        Assert.assertEquals(length,
                            StringEncoding.writeAsciiString(buf, 0, str));
        Assert.assertArrayEquals(Bytes.fromHex("80"), buf);
    }

    @Test
    public void testReadAsciiString() {
        byte[] buf = Bytes.fromHex("6162633133b3");
        Assert.assertEquals("abc133", StringEncoding.readAsciiString(buf, 0));

        buf = Bytes.fromHex("80");
        Assert.assertEquals("", StringEncoding.readAsciiString(buf, 0));
    }

    @Test
    public void testCheckPasswordSupportsOldAndNewCost() {
        String testPassword = "test123!@#";

        // Test old work factor (4)
        String oldPassword = BCrypt.hashpw(testPassword, BCrypt.gensalt(4));
        Assert.assertTrue(StringEncoding.checkPassword(testPassword, oldPassword));
        Assert.assertFalse(StringEncoding.checkPassword("wrong", oldPassword));

        // Test new work factor (10)
        String newPassword = BCrypt.hashpw(testPassword, BCrypt.gensalt(10));
        Assert.assertTrue(StringEncoding.checkPassword(testPassword, newPassword));
        Assert.assertFalse(StringEncoding.checkPassword("wrong", newPassword));

        // Test that hashPassword uses the new cost factor
        String hashedPassword = StringEncoding.hashPassword(testPassword);
        Assert.assertTrue("Hash should contain work factor 10",
                          hashedPassword.matches("^\\$2[aby]\\$10\\$.*")
        );

        // Compare computational cost between work factor 4 and 10
        long start4 = System.nanoTime();
        StringEncoding.checkPassword(testPassword, oldPassword);
        long elapsed4 = System.nanoTime() - start4;

        long start10 = System.nanoTime();
        StringEncoding.checkPassword(testPassword, hashedPassword);
        long elapsed10 = System.nanoTime() - start10;

        // BCrypt cost difference: (10-4) = 6 => theoretical ~2^6 = 64x
        Assert.assertTrue(
                "Work factor 10 should be significantly slower than work factor 4 " +
                "(expected exponential cost increase)",
                elapsed10 >= elapsed4 * 32
        );
    }
}
