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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IpUtil {

    /**
     * Get local IPv4 addresses.
     *
     * @param forFallback whether link-local and loopback addresses are excluded
     * @return ipv4 addr
     * @throws SocketException io error or no network interface
     */
    private static List<String> getIpAddress(boolean forFallback) throws SocketException {
        List<String> list = new LinkedList<>();
        Enumeration enumeration = NetworkInterface.getNetworkInterfaces();
        while (enumeration.hasMoreElements()) {
            NetworkInterface network = (NetworkInterface) enumeration.nextElement();
            Enumeration addresses = network.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = (InetAddress) addresses.nextElement();
                if (!(address instanceof Inet4Address)) {
                    continue;
                }
                if (forFallback && (address.isLoopbackAddress() ||
                                    address.isLinkLocalAddress())) {
                    continue;
                }
                list.add(address.getHostAddress());
            }
        }
        return list;
    }

    /**
     * According to the raft addr in the option, get the closest one based on the local IP.
     *
     * @param raftAddress raft addr
     * @return raft addr that have the nearest distance with given param
     */
    public static String getNearestAddress(String raftAddress) {
        try {
            String[] tmp = raftAddress.split(":");
            if (!isDottedIpv4(tmp[0])) {
                return raftAddress;
            }

            // A configured literal that is bound locally stays, including
            // loopback. The default raft address is 127.0.0.1 and the
            // partition engine uses that same value as its PeerId.
            // Link-local and loopback are dropped only from fallback
            // candidates, when the configured IPv4 is not local.
            if (getIpAddress(false).contains(tmp[0])) {
                return raftAddress;
            }

            List<String> ipv4s = getIpAddress(true);
            if (ipv4s.size() == 0) {
                throw new Exception("no available ipv4");
            }

            if (ipv4s.size() == 1) {
                return ipv4s.get(0) + ":" + tmp[1];
            }

            var raftSeg = Arrays.stream(tmp[0].split("\\."))
                                .map(s -> Integer.parseInt(s))
                                .collect(Collectors.toList());

            ipv4s.sort(Comparator.comparingLong(ip -> {
                String[] ipSegments = ip.split("\\.");
                long base = 256 * 256 * 256;
                int i = 0;
                long sum = 0;
                for (String seg : ipSegments) {
                    sum += base * (Math.abs(raftSeg.get(i) - Integer.parseInt(seg)));
                    base = base / 256;
                    i += 1;
                }
                return sum;
            }));

            return ipv4s.get(0) + ":" + tmp[1];
        } catch (SocketException e) {
            log.error("getIpAddress, get ip failed, {}", e.getMessage());
        } catch (Exception e) {
            log.error("getNearestAddress, got exception, {}", e.getMessage());
        }
        return raftAddress;
    }

    /**
     * A dotted IPv4 literal is four numeric octets. Hostnames must not be
     * parsed as addresses; {@code Integer.parseInt} on a DNS label is not a
     * signal to log or to replace the configured host.
     */
    private static boolean isDottedIpv4(String host) {
        String[] parts = host.split("\\.", -1);
        if (parts.length != 4) {
            return false;
        }
        for (String part : parts) {
            if (part.isEmpty() || part.length() > 3) {
                return false;
            }
            for (int i = 0; i < part.length(); i++) {
                if (!Character.isDigit(part.charAt(i))) {
                    return false;
                }
            }
            if (Integer.parseInt(part) > 255) {
                return false;
            }
        }
        return true;
    }
}
