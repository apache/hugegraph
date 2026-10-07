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

package org.apache.hugegraph.backend.page;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.StringEncoding;

public class PageState {

    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final PageState EMPTY = new PageState(EMPTY_BYTES, 0, 0);
    public static final char SPACE = ' ';
    public static final char PLUS = '+';

    private final byte[] position;
    private final int offset;
    private final int total;

    public PageState(byte[] position, int offset, int total) {
        E.checkNotNull(position, "position");
        this.position = position;
        this.offset = offset;
        this.total = total;
    }

    public byte[] position() {
        return this.position;
    }

    public int offset() {
        return this.offset;
    }

    public long total() {
        return this.total;
    }

    @Override
    public String toString() {
        if (Bytes.equals(this.position(), EMPTY_BYTES)) {
            return null;
        }
        return toString(this.toBytes());
    }

    private byte[] toBytes() {
        assert this.position.length > 0;
        int length = 2 + this.position.length + 2 * BytesBuffer.INT_LEN;
        BytesBuffer buffer = BytesBuffer.allocate(length);
        buffer.writeBytes(this.position);
        buffer.writeInt(this.offset);
        buffer.writeInt(this.total);
        return buffer.bytes();
    }

    public static PageState fromString(String page) {
        E.checkNotNull(page, "page");
        /*
         * URLDecoder will auto decode '+' to space in url due to the request
         * of HTML4, so we choose to replace the space to '+' after getting it
         * More details refer to #1437
         */
        page = page.replace(SPACE, PLUS);
        return fromBytes(toBytes(page));
    }

    public static PageState fromBytes(byte[] bytes) {
        if (bytes.length == 0) {
            // The first page
            return EMPTY;
        }
        try {
            BytesBuffer buffer = BytesBuffer.wrap(bytes);
            return new PageState(buffer.readBytes(), buffer.readInt(),
                                 buffer.readInt());
        } catch (Exception e) {
            throw new BackendException("Invalid page: '0x%s'",
                                       e, Bytes.toHex(bytes));
        }
    }

    public static String toString(byte[] bytes) {
        return StringEncoding.encodeBase64(bytes);
    }

    public static byte[] toBytes(String page) {
        try {
            return StringEncoding.decodeBase64(page);
        } catch (Exception e) {
            throw new BackendException("Invalid page: '%s'", e, page);
        }
    }
}
