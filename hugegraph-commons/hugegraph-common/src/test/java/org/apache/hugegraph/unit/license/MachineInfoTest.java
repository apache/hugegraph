/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.license;

import java.net.InetAddress;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.hugegraph.license.MachineInfo;
import org.apache.hugegraph.testutil.Assert;

public class MachineInfoTest {

    private static final Pattern IPV4_PATTERN = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}" +
            "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$"
    );
    private static final Pattern MAC_PATTERN = Pattern.compile(
            "^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$"
    );

    private static final MachineInfo machineInfo = new MachineInfo();

    @Test
    public void testGetIpAddressList() {
        List<String> ipAddressList = machineInfo.getIpAddress();
        for (String ip : ipAddressList) {
            Assert.assertTrue(isIpAddress(ip));
        }
        Assert.assertEquals(ipAddressList, machineInfo.getIpAddress());
    }

    @Test
    public void testGetMacAddressList() {
        List<String> macAddressList = machineInfo.getMacAddress();
        for (String mac : macAddressList) {
            Assert.assertTrue(MAC_PATTERN.matcher(mac).matches());
        }
        Assert.assertEquals(macAddressList, machineInfo.getMacAddress());
    }

    @Test
    public void testGetLocalAllInetAddress() {
        List<InetAddress> addressList = machineInfo.getLocalAllInetAddress();
        for (InetAddress address : addressList) {
            String ip = address.getHostAddress();
            Assert.assertTrue(isIpAddress(ip));
        }
    }

    @Test
    public void testScopedIpv6AddressValidation() throws UnknownHostException {
        byte[] bytes = new byte[16];
        bytes[0] = 0x20;
        bytes[1] = 0x01;
        bytes[2] = 0x0d;
        bytes[3] = (byte) 0xb8;
        bytes[15] = 1;
        Inet6Address scoped = Inet6Address.getByAddress(null, bytes, 7);
        Assert.assertTrue(isIpAddress(scoped.getHostAddress()));
        Inet6Address parsed = (Inet6Address) InetAddress.getByName(scoped.getHostAddress());
        Assert.assertArrayEquals(bytes, parsed.getAddress());
        Assert.assertEquals(7, parsed.getScopeId());
        Assert.assertTrue(isIpAddress("2001:db8::1"));
        Assert.assertTrue(isIpAddress("192.0.2.1"));
    }

    @Test
    public void testRejectMalformedIpAddresses() {
        for (String ip : new String[]{"localhost", "256.0.0.1", "2001:db8::gg",
                                      "2001::db8::1", "localhost:8080"}) {
            Assert.assertFalse(isIpAddress(ip));
        }
    }

    private static boolean isIpAddress(String ip) {
        if (IPV4_PATTERN.matcher(ip).matches()) {
            return true;
        }
        // Only parse IPv6 literals here; never resolve a hostname through DNS.
        if (!ip.contains(":")) {
            return false;
        }
        try {
            return InetAddress.getByName(ip) instanceof Inet6Address;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    @Test
    public void testGetMacByInetAddress() throws UnknownHostException {
        List<InetAddress> addressList = machineInfo.getLocalAllInetAddress();
        for (InetAddress address : addressList) {
            String mac = machineInfo.getMacByInetAddress(address);
            Assert.assertTrue(MAC_PATTERN.matcher(mac).matches());
        }
        InetAddress address = InetAddress.getByAddress(new byte[]{0, 0, 0, 0});
        Assert.assertThrows(RuntimeException.class, () -> {
            machineInfo.getMacByInetAddress(address);
        });
    }
}
