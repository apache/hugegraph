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

package org.apache.hugegraph.store.util;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@Deprecated
public class Base58Encoder {

    public static final char[] CHAR_SET =
            "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".toCharArray();
    private static final int[] CHAR_INDEXES = new int[128];

    static {
        Arrays.fill(CHAR_INDEXES, -1);
        for (int i = 0; i < CHAR_SET.length; i++) {
            CHAR_INDEXES[CHAR_SET[i]] = i;
        }
    }

    /**
     * Converts the input bytes to a Base58 encoded string without checksum.
     */
    public static String convertToBase58(byte[] byteArray) {
        if (byteArray.length == 0) {
            return "";
        }
        byteArray = getSubArray(byteArray, 0, byteArray.length);
        // Count leading zeros.
        int leadingZeroCount = 0;
        while (leadingZeroCount < byteArray.length && byteArray[leadingZeroCount] == 0) {
            ++leadingZeroCount;
        }
        // Actual encoding process.
        byte[] intermediate = new byte[byteArray.length * 2];
        int index = intermediate.length;

        int position = leadingZeroCount;
        while (position < byteArray.length) {
            byte remainder = divideAndModulo58(byteArray, position);
            if (byteArray[position] == 0) {
                ++position;
            }
            intermediate[--index] = (byte) CHAR_SET[remainder];
        }

        // Remove extra leading '1's.
        while (index < intermediate.length && intermediate[index] == CHAR_SET[0]) {
            ++index;
        }
        // Add leading '1's based on the count of leading zeros.
        while (--leadingZeroCount >= 0) {
            intermediate[--index] = (byte) CHAR_SET[0];
        }

        byte[] result = getSubArray(intermediate, index, intermediate.length);
        return new String(result, StandardCharsets.US_ASCII);
    }

    public static byte[] convertFromBase58(String encodedStr) throws IllegalArgumentException {
        if (encodedStr.isEmpty()) {
            return new byte[0];
        }
        byte[] encodedBytes = new byte[encodedStr.length()];
        // Transform the string into a byte array based on Base58
        for (int i = 0; i < encodedStr.length(); ++i) {
            char character = encodedStr.charAt(i);

            int value58 = -1;
            if (character < 128) {
                value58 = CHAR_INDEXES[character];
            }
            if (value58 < 0) {
                throw new IllegalArgumentException("Invalid character " + character + " at " + i);
            }

            encodedBytes[i] = (byte) value58;
        }
        // Count leading zeros
        int leadingZeroCount = 0;
        while (leadingZeroCount < encodedBytes.length && encodedBytes[leadingZeroCount] == 0) {
            ++leadingZeroCount;
        }
        // Decoding process
        byte[] intermediate = new byte[encodedStr.length()];
        int index = intermediate.length;

        int position = leadingZeroCount;
        while (position < encodedBytes.length) {
            byte remainder = divideAndModulo256(encodedBytes, position);
            if (encodedBytes[position] == 0) {
                ++position;
            }

            intermediate[--index] = remainder;
        }
        // Avoid adding extra leading zeros, adjust index to first non-zero byte.
        while (index < intermediate.length && intermediate[index] == 0) {
            ++index;
        }

        return getSubArray(intermediate, index - leadingZeroCount, intermediate.length);
    }

    public static BigInteger convertToBigInt(String encodedStr) throws IllegalArgumentException {
        return new BigInteger(1, convertFromBase58(encodedStr));
    }

    //
    // number -> number / 58, returns number % 58
    //
    private static byte divideAndModulo58(byte[] number, int startIndex) {
        int remainder = 0;
        for (int i = startIndex; i < number.length; i++) {
            int byteValue = (int) number[i] & 0xFF;
            int temp = remainder * 256 + byteValue;

            number[i] = (byte) (temp / 58);

            remainder = temp % 58;
        }

        return (byte) remainder;
    }

    //
    // number -> number / 256, returns number % 256
    //
    private static byte divideAndModulo256(byte[] number58, int startIndex) {
        int remainder = 0;
        for (int i = startIndex; i < number58.length; i++) {
            int byteValue58 = (int) number58[i] & 0xFF;
            int temp = remainder * 58 + byteValue58;

            number58[i] = (byte) (temp / 256);

            remainder = temp % 256;
        }

        return (byte) remainder;
    }

    private static byte[] getSubArray(byte[] sourceArray, int start, int end) {
        byte[] resultArray = new byte[end - start];
        System.arraycopy(sourceArray, start, resultArray, 0, resultArray.length);

        return resultArray;
    }
}
