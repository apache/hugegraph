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

package org.apache.hugegraph.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hugegraph.exception.BackendException;
import org.junit.Assert;
import org.junit.Test;

public class LZ4UtilTest {

    @Test
    public void testCompressThenDecompressRoundTrips() {
        byte[] input = new byte[10000];
        for (int i = 0; i < input.length; i++) {
            input[i] = (byte) (i % 31);
        }

        byte[] compressed = LZ4Util.compress(input, 1024).bytes();
        Assert.assertTrue(compressed.length < input.length);

        byte[] output = LZ4Util.decompress(compressed, 1024).bytes();
        Assert.assertArrayEquals(input, output);
    }

    @Test
    public void testInvalidBufferRatioFallsBackToDefault() {
        byte[] input = "hugegraph".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = LZ4Util.compress(input, 64, 0.0F).bytes();
        byte[] output = LZ4Util.decompress(compressed, 64, -1.0F).bytes();
        Assert.assertArrayEquals(input, output);
    }

    @Test
    public void testDecompressCorruptInputThrows() {
        byte[] compressed = LZ4Util.compress(new byte[256], 64).bytes();
        byte[] corrupt = Arrays.copyOf(compressed, compressed.length / 2);
        Assert.assertThrows(BackendException.class, () -> LZ4Util.decompress(corrupt, 64));
    }
}
