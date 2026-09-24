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

package org.apache.hugegraph.pd.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Test;

public class StoreRestAddressUtilTest {

    @Test
    public void testBuildIPv4RestAddress() {
        Metapb.Store store = store("127.0.0.1:8500", "8520");

        assertEquals("127.0.0.1:8520",
                     StoreRestAddressUtil.getRestAddress(store));
    }

    @Test
    public void testBuildIPv6RestAddress() {
        Metapb.Store store = store("[2001:db8::1]:8500", "8520");

        assertEquals("[2001:db8::1]:8520",
                     StoreRestAddressUtil.getRestAddress(store));
    }

    @Test
    public void testBuildRestAddressFromUri() {
        Metapb.Store store = store("http://store.example:8500", "8520");

        assertEquals("store.example:8520",
                     StoreRestAddressUtil.getRestAddress(store));
    }

    @Test
    public void testRejectMissingOrInvalidRestPort() {
        assertNull(StoreRestAddressUtil.getRestAddress(
                store("127.0.0.1:8500", null)));
        assertNull(StoreRestAddressUtil.getRestAddress(
                store("127.0.0.1:8500", "0")));
        assertNull(StoreRestAddressUtil.getRestAddress(
                store("127.0.0.1:8500", "65536")));
        assertNull(StoreRestAddressUtil.getRestAddress(
                store("127.0.0.1:8500", "8520/path")));
    }

    @Test
    public void testRejectMissingOrMalformedStoreAddress() {
        assertNull(StoreRestAddressUtil.getRestAddress(store("", "8520")));
        assertNull(StoreRestAddressUtil.getRestAddress(
                store("2001:db8::1:8500", "8520")));
    }

    private static Metapb.Store store(String address, String restPort) {
        Metapb.Store.Builder builder = Metapb.Store.newBuilder()
                                                   .setAddress(address);
        if (restPort != null) {
            builder.addLabels(Metapb.StoreLabel.newBuilder()
                                               .setKey("rest.port")
                                               .setValue(restPort));
        }
        return builder.build();
    }
}
