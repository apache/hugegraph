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

package org.apache.hugegraph.store.rocksdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.nio.charset.StandardCharsets;

import org.apache.hugegraph.rocksdb.access.util.CRC64;
import org.junit.Test;

public class CRC64Test {

    private static final byte[] CHECK_INPUT = "123456789".getBytes(StandardCharsets.US_ASCII);

    @Test
    public void testMatchesCrc64Ecma182CheckValue() {
        CRC64 crc = new CRC64();
        assertEquals(0L, crc.getValue());
        crc.update(CHECK_INPUT);
        // Standard check value of CRC-64/ECMA-182 (poly 0x42F0E1EBA9EA3693, init 0, no reflection)
        assertEquals(0x6C40DF5F0B497347L, crc.getValue());
    }

    @Test
    public void testAllUpdateOverloadsAreEquivalent() {
        CRC64 whole = new CRC64();
        whole.update(CHECK_INPUT);

        CRC64 perInt = new CRC64();
        for (byte b : CHECK_INPUT) {
            // update(int) must only consider the low eight bits
            perInt.update(b | 0xABCD00);
        }

        CRC64 split = new CRC64();
        split.update(CHECK_INPUT, 0, 4);
        split.update(CHECK_INPUT, 4, CHECK_INPUT.length - 4);

        assertEquals(whole.getValue(), perInt.getValue());
        assertEquals(whole.getValue(), split.getValue());
    }

    @Test
    public void testOffsetAndLengthSelectOnlyTheRequestedSlice() {
        byte[] padded = new byte[CHECK_INPUT.length + 6];
        padded[0] = 'x';
        padded[1] = 'y';
        padded[2] = 'z';
        System.arraycopy(CHECK_INPUT, 0, padded, 3, CHECK_INPUT.length);
        padded[padded.length - 1] = 'q';

        CRC64 crc = new CRC64();
        crc.update(padded, 3, CHECK_INPUT.length);
        assertEquals(0x6C40DF5F0B497347L, crc.getValue());

        CRC64 empty = new CRC64();
        empty.update(padded, 5, 0);
        assertEquals(0L, empty.getValue());
    }

    @Test
    public void testResetAndSensitivityToSingleBitChanges() {
        CRC64 crc = new CRC64();
        crc.update(CHECK_INPUT);
        long expected = crc.getValue();

        byte[] flipped = CHECK_INPUT.clone();
        flipped[4] ^= 0x01;
        crc.reset();
        assertEquals(0L, crc.getValue());
        crc.update(flipped);
        assertNotEquals(expected, crc.getValue());

        crc.reset();
        crc.update(CHECK_INPUT);
        assertEquals(expected, crc.getValue());
    }
}
