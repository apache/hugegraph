/*
 * Copyright 2017 HugeGraph Authors
 *
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

package org.apache.hugegraph.pd.license;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.license.MachineInfo;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.springframework.stereotype.Service;
import org.springframework.util.Base64Utils;

import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LicenseVerifierService {

    private PDConfig pdConfig;
    private final MachineInfo machineInfo;

    public LicenseVerifierService(PDConfig pdConfig) {
        this.pdConfig = pdConfig;
        machineInfo = new MachineInfo();
    }

    @Deprecated
    public void init() {
    }

    public synchronized void install(String md5) {
    }

    @Deprecated
    public HashMap getContext() throws Exception {
        return new HashMap();
    }

    @Deprecated
    public void verify(int cores, int nodeCount) {
    }

    public boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    public String getIpAndMac() {
        List<String> actualIps = this.machineInfo.getIpAddress();
        String host = pdConfig.getHost();
        String licenseHost = host;
        if (!actualIps.contains(host)) {
            licenseHost = actualIps.get(0);
        }
        try {
            String mac = this.machineInfo.getMacByInetAddress(InetAddress.getByName(licenseHost));
            HashMap<String, String> ipAndMac = new HashMap<>();
            ipAndMac.put("ip", licenseHost);
            ipAndMac.put("mac", mac);
            String json = new Gson().toJson(ipAndMac);
            String encode = Base64Utils.encodeToString(json.getBytes(Charset.defaultCharset()));
            return encode;
        } catch (Exception e) {
            throw new PDRuntimeException(Pdpb.ErrorType.LICENSE_ERROR_VALUE,
                                         String.format("Failed to get ip and mac for %s",
                                                       e.getMessage()));
        }
    }

    private void checkIpAndMac(ExtraParam param) {
        String expectIp = param.ip();
        boolean matched = false;
        List<String> actualIps = null;
        if (StringUtils.isEmpty(expectIp)) {
            matched = true;
        } else {
            actualIps = this.machineInfo.getIpAddress();
            for (String actualIp : actualIps) {
                if (actualIp.equalsIgnoreCase(expectIp)) {
                    matched = true;
                    break;
                }
            }
        }
        if (!matched) {
            throw new PDRuntimeException(Pdpb.ErrorType.LICENSE_VERIFY_ERROR_VALUE, String.format(
                    "The server's ip '%s' doesn't match the authorized '%s'", actualIps, expectIp));
        }
        String expectMac = param.mac();
        if (StringUtils.isEmpty(expectMac)) {
            return;
        }
        // The mac must be not empty here
        if (!StringUtils.isEmpty(expectIp)) {
            String actualMac;
            try {
                actualMac = this.machineInfo.getMacByInetAddress(InetAddress.getByName(expectIp));
            } catch (UnknownHostException e) {
                throw new PDRuntimeException(Pdpb.ErrorType.LICENSE_VERIFY_ERROR_VALUE,
                                             String.format("Failed to get mac address for ip '%s'",
                                                           expectIp));
            }
            String expectFormatMac = expectMac.replaceAll(":", "-");
            String actualFormatMac = actualMac.replaceAll(":", "-");
            if (!actualFormatMac.equalsIgnoreCase(expectFormatMac)) {
                throw new PDRuntimeException(Pdpb.ErrorType.LICENSE_VERIFY_ERROR_VALUE, String.format(
                        "The server's mac '%s' doesn't match the authorized '%s'", actualMac, expectMac));
            }
        } else {
            String expectFormatMac = expectMac.replaceAll(":", "-");
            List<String> actualMacs = this.machineInfo.getMacAddress();
            matched = false;
            for (String actualMac : actualMacs) {
                String actualFormatMac = actualMac.replaceAll(":", "-");
                if (actualFormatMac.equalsIgnoreCase(expectFormatMac)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                throw new PDRuntimeException(Pdpb.ErrorType.LICENSE_VERIFY_ERROR_VALUE, String.format(
                        "The server's macs %s don't match the authorized '%s'", actualMacs, expectMac));
            }
        }
    }
}
