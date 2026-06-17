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

package org.apache.hugegraph.store.node.controller;

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;
import org.apache.hugegraph.store.node.metrics.DriveMetrics;
import org.apache.hugegraph.store.node.metrics.SystemMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.alipay.sofa.jraft.core.NodeMetrics;

/**
 * 2021/11/23
 */
@RestController
@RequestMapping(value = "/metrics", method = RequestMethod.GET)
public class HgStoreMetricsController {

    private final SystemMetrics systemMetrics = new SystemMetrics();
    private final DriveMetrics driveMetrics = new DriveMetrics();
    @Autowired
    HgStoreNodeService nodeService;

    @GetMapping
    public Map<String, String> index() {
        return new HashMap<>();
    }

    @GetMapping("system")
    public Map<String, Map<String, Object>> system() {
        return this.systemMetrics.metrics();
    }

    @GetMapping("drive")
    public Map<String, Map<String, Object>> drive() {
        return this.driveMetrics.metrics();
    }

    @GetMapping("raft")
    public Map<String, NodeMetrics> getRaftMetrics() {
        return nodeService.getNodeMetrics();
    }

    @GetMapping("leases")
    public Map<String, Object> getLeaseMetrics() {
        return nodeService.getPartitionLeaseMetrics();
    }

}
