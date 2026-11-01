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

package org.apache.hugegraph.license;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MachineInfo {

    private List<String> ipAddressList;
    private List<String> macAddressList;

    public MachineInfo() {
        this.ipAddressList = null;
        this.macAddressList = null;
    }

    public List<String> getIpAddress() {
        if (this.ipAddressList != null) {
            return this.ipAddressList;
        }
        this.ipAddressList = new ArrayList<>();
        List<InetAddress> inetAddresses = this.getLocalAllInetAddress();
        if (inetAddresses != null && !inetAddresses.isEmpty()) {
            this.ipAddressList = inetAddresses.stream()
                                              .map(InetAddress::getHostAddress)
                                              .distinct()
                                              .map(String::toLowerCase)
                                              .collect(Collectors.toList());
        }
        return this.ipAddressList;
    }

    public List<String> getMacAddress() {
        if (this.macAddressList != null) {
            return this.macAddressList;
        }
        List<String> list = new ArrayList<>();
        List<InetAddress> inetAddresses = this.getLocalAllInetAddress();
        if (inetAddresses != null && !inetAddresses.isEmpty()) {
            // Get the Mac address of all network interfaces
            Set<String> uniqueValues = new HashSet<>();
            for (InetAddress inetAddress : inetAddresses) {
                String macByInetAddress = this.getMacByInetAddress(inetAddress, false);
                if (macByInetAddress != null && uniqueValues.add(macByInetAddress)) {
                    list.add(macByInetAddress);
                }
            }
        }
        this.macAddressList = list;
        return this.macAddressList;
    }

    public List<InetAddress> getLocalAllInetAddress() {
        Enumeration<NetworkInterface> interfaces;
        try {
            interfaces = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            throw new RuntimeException("Failed to get network interfaces");
        }

        List<InetAddress> result = new ArrayList<>();
        while (interfaces.hasMoreElements()) {
            NetworkInterface nw = interfaces.nextElement();
            for (Enumeration<InetAddress> inetAddresses = nw.getInetAddresses();
                 inetAddresses.hasMoreElements(); ) {
                InetAddress inetAddr = inetAddresses.nextElement();
                if (!inetAddr.isLoopbackAddress() &&
                    !inetAddr.isLinkLocalAddress() &&
                    !inetAddr.isMulticastAddress()) {
                    result.add(inetAddr);
                }
            }
        }
        return result;
    }

    public String getMacByInetAddress(InetAddress inetAddr) {
        return this.getMacByInetAddress(inetAddr, true);
    }

    private String getMacByInetAddress(InetAddress inetAddr, boolean required) {
        byte[] mac;
        try {
            mac = NetworkInterface.getByInetAddress(inetAddr)
                                  .getHardwareAddress();
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                      "Failed to get mac address for inet address '%s'",
                      inetAddr));
        }
        if (mac == null) {
            if (required) {
                throw new IllegalStateException("Network interface has no hardware address");
            }
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mac.length; i++) {
            if (i != 0) {
                sb.append("-");
            }
            String temp = Integer.toHexString(mac[i] & 0xff);
            if (temp.length() == 1) {
                sb.append("0").append(temp);
            } else {
                sb.append(temp);
            }
        }
        return sb.toString().toUpperCase();
    }
}
