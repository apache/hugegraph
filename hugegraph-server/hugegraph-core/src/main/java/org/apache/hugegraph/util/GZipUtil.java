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

import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.serializer.BytesBuffer;

public final class GZipUtil {

    private static final int BUF_SIZE = (int) (4 * Bytes.KB);

    public static String md5(String input) {
        return DigestUtils.md5Hex(input);
    }

    public static BytesBuffer compress(byte[] data) {
        int estimateSize = data.length >> 3;
        BytesBuffer output = BytesBuffer.allocate(estimateSize);

        Deflater deflater = new Deflater();
        deflater.setInput(data);
        deflater.finish();
        byte[] buffer = new byte[BUF_SIZE];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            output.write(buffer, 0, count);
        }
        output.forReadWritten();
        return output;
    }

    public static BytesBuffer decompress(byte[] data) {
        int estimateSize = data.length << 3;
        BytesBuffer output = BytesBuffer.allocate(estimateSize);

        Inflater inflater = new Inflater();
        inflater.setInput(data);
        byte[] buffer = new byte[BUF_SIZE];
        while (!inflater.finished()) {
            try {
                int count = inflater.inflate(buffer);
                output.write(buffer, 0, count);
            } catch (DataFormatException e) {
                throw new BackendException("Failed to decompress", e);
            }
        }
        output.forReadWritten();
        return output;
    }
}
