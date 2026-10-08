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

package org.apache.hugegraph.store.core.store.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.hugegraph.store.util.IpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class IpUtilTest {

    @Test
    public void testConfiguredLoopbackStays() throws SocketException {
        Assume.assumeTrue(hasLocalIpv4("127.0.0.1", true));
        Assert.assertEquals("127.0.0.1:8510",
                            IpUtil.getNearestAddress("127.0.0.1:8510"));
    }

    @Test
    public void testNearestAddressDoesNotPreferLoopback() throws SocketException {
        Assume.assumeTrue(hasLocalIpv4(null, false));
        String selected = IpUtil.getNearestAddress("127.0.0.2:8510");
        Assert.assertNotEquals("127.0.0.1:8510", selected);
        Assert.assertFalse(selected.startsWith("127."));
    }

    @Test
    public void testConfiguredLinkLocalStays() throws SocketException {
        String linkLocal = findLocalLinkLocalIpv4();
        Assume.assumeTrue(linkLocal != null);
        Assert.assertEquals(linkLocal + ":8510",
                            IpUtil.getNearestAddress(linkLocal + ":8510"));
    }

    @Test
    public void testHostnameIsReturnedWithoutError() {
        Logger logger = (Logger) LogManager.getLogger(IpUtil.class);
        MemoryAppender appender = new MemoryAppender();
        appender.start();
        Level previous = logger.getLevel();
        logger.addAppender(appender);
        logger.setLevel(Level.ERROR);
        try {
            Assert.assertEquals("store0:8500",
                                IpUtil.getNearestAddress("store0:8500"));
            Assert.assertEquals("hugegraph-store-0.svc.cluster.local:8510",
                                IpUtil.getNearestAddress(
                                        "hugegraph-store-0.svc.cluster.local:8510"));
            Assert.assertTrue(appender.errors.isEmpty());
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previous);
            appender.stop();
        }
    }

    private static String findLocalLinkLocalIpv4() throws SocketException {
        Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
        if (nics == null) {
            return null;
        }
        while (nics.hasMoreElements()) {
            NetworkInterface nic = nics.nextElement();
            Enumeration<InetAddress> addresses = nic.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (address instanceof Inet4Address && address.isLinkLocalAddress()) {
                    return address.getHostAddress();
                }
            }
        }
        return null;
    }

    private static boolean hasLocalIpv4(String expected, boolean includeLoopback)
            throws SocketException {
        Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
        if (nics == null) {
            return false;
        }
        while (nics.hasMoreElements()) {
            NetworkInterface nic = nics.nextElement();
            Enumeration<InetAddress> addresses = nic.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (!(address instanceof Inet4Address) || address.isLinkLocalAddress()) {
                    continue;
                }
                if (!includeLoopback && address.isLoopbackAddress()) {
                    continue;
                }
                if (expected == null || expected.equals(address.getHostAddress())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static final class MemoryAppender extends AbstractAppender {

        private final List<String> errors = new ArrayList<>();

        private MemoryAppender() {
            super("iputil-test", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
                errors.add(event.getMessage().getFormattedMessage());
            }
        }
    }
}
