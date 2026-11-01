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

package org.apache.hugegraph.store.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.hugegraph.store.client.util.Base58;
import org.apache.hugegraph.store.util.Base58Encoder;
import org.junit.Test;

public class Base58EncoderTest {

    @Test
    public void testKnownVectors() {
        assertEquals("", Base58Encoder.convertToBase58(new byte[0]));
        assertEquals("2NEpo7TZRRrLZSi2U",
                     Base58Encoder.convertToBase58("Hello World!".getBytes(StandardCharsets.US_ASCII)));
        assertEquals("5Q", Base58Encoder.convertToBase58(new byte[]{(byte) 0xff}));
        assertArrayEquals("Hello World!".getBytes(StandardCharsets.US_ASCII),
                          Base58Encoder.convertFromBase58("2NEpo7TZRRrLZSi2U"));
        assertArrayEquals(new byte[0], Base58Encoder.convertFromBase58(""));
    }

    @Test
    public void testLeadingZeroBytesBecomeLeadingOnes() {
        assertEquals("1", Base58Encoder.convertToBase58(new byte[]{0}));
        assertEquals("111", Base58Encoder.convertToBase58(new byte[]{0, 0, 0}));
        assertEquals("112", Base58Encoder.convertToBase58(new byte[]{0, 0, 1}));

        assertArrayEquals(new byte[]{0}, Base58Encoder.convertFromBase58("1"));
        assertArrayEquals(new byte[]{0, 0, 1}, Base58Encoder.convertFromBase58("112"));
    }

    @Test
    public void testRoundTripDoesNotMutateInput() {
        Random random = new Random(20261011L);
        for (int i = 0; i < 200; i++) {
            byte[] input = new byte[random.nextInt(40)];
            random.nextBytes(input);
            if (input.length > 0 && random.nextBoolean()) {
                input[0] = 0;
            }
            byte[] snapshot = input.clone();

            String encoded = Base58Encoder.convertToBase58(input);

            assertArrayEquals("encoding must not modify its argument", snapshot, input);
            assertArrayEquals(input, Base58Encoder.convertFromBase58(encoded));
            // Store-side and client-side encoders must agree on the wire representation
            assertEquals(Base58.encode(input), encoded);
        }
    }

    @Test
    public void testConvertToBigIntIgnoresLeadingZeros() {
        assertEquals(BigInteger.ZERO, Base58Encoder.convertToBigInt("1"));
        assertEquals(BigInteger.valueOf(57), Base58Encoder.convertToBigInt("z"));
        assertEquals(BigInteger.valueOf(58), Base58Encoder.convertToBigInt("121"));
    }

    @Test
    public void testRejectsCharactersOutsideTheAlphabet() {
        // '0', 'O', 'I' and 'l' are excluded from the Base58 alphabet to avoid ambiguity
        for (String invalid : new String[]{"0", "O", "I", "l", "2N+", "abc def", "é"}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                                                      () -> Base58Encoder.convertFromBase58(invalid));
            assertTrue(e.getMessage(), e.getMessage().startsWith("Invalid character"));
        }
        assertThrows(IllegalArgumentException.class, () -> Base58Encoder.convertToBigInt("0"));
    }
}
