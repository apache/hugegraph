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

package org.apache.hugegraph.unit.core;

import java.util.Arrays;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class PageStateTest {

    private String pageWith3Base64Chars = "AAAAADsyABwAEAqI546LS6WW57unBgA" +
                                          "EAAAAAPB////+8H////4alhxAZS8va6" +
                                          "opcAKpklipAAQAAAAAAAAAAQ==";

    private String pageWithSpace = "AAAAADsyABwAEAqI546LS6WW57unBgAEAAAAAP" +
                                   "B//// 8H////4alhxAZS8va6opcAKpklipAAQA" +
                                   "AAAAAAAAAQ==";

    @Test
    public void testOriginalStringPageToBytes() {
        byte[] validPage = PageState.toBytes(pageWith3Base64Chars);
        Assert.assertNotNull(validPage);

        Assert.assertThrows(BackendException.class, () -> {
            PageState.toBytes(pageWithSpace);
        }, e -> {
            Assert.assertContains("Invalid page:", e.toString());
        });
    }

    @Test
    public void testDecodePageWithSpecialBase64Chars() {
        // Assert decode '+' and '/' and '=' and space successfully
        Assert.assertNotNull(PageState.fromString(pageWith3Base64Chars));

        byte[] decodePlus = PageState.fromString(pageWith3Base64Chars)
                                     .position();
        byte[] decodeSpace = PageState.fromString(pageWithSpace).position();

        Assert.assertTrue(Arrays.equals(decodePlus, decodeSpace));
    }

    @Test
    public void testDecodePageWithInvalidStringPage() {
        final String invalidPageWithBase64Chars = "dGVzdCBiYXNlNjQ=";

        Assert.assertThrows(BackendException.class, () -> {
            PageState.fromString(invalidPageWithBase64Chars);
        }, e -> {
            Assert.assertContains("Invalid page: '0x", e.toString());
        });

        final String invalidBase64Chars = "!abc~";
        Assert.assertThrows(BackendException.class, () -> {
            PageState.fromString(invalidBase64Chars);
        }, e -> {
            Assert.assertContains("Invalid page:", e.toString());
        });
    }

    @Test
    public void testEmptyPageState() {
        Assert.assertEquals(0, PageState.EMPTY.offset());
        Assert.assertNull(PageState.EMPTY.toString());

        Assert.assertEquals(PageState.EMPTY,
                            PageState.fromBytes(PageState.EMPTY_BYTES));
    }
}
