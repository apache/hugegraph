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

package org.apache.hugegraph.pd.rest;

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.service.PDRestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/v1/task")
public class TaskAPI extends API {

    @Autowired
    PDRestService pdRestService;

    @GetMapping(value = "/patrolStores", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String patrolStores() {
        try {
            List<Metapb.Store> stores = pdRestService.patrolStores();
            return toJSON(stores, "stores");
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }

    @GetMapping(value = "/patrolPartitions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String patrolPartitions() {
        try {
            List<Metapb.Partition> partitions = pdRestService.patrolPartitions();
            return toJSON(partitions, "partitions");
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }

    @GetMapping(value = "/balancePartitions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<Integer, KVPair<Long, Long>> balancePartitions() {
        try {
            Map<Integer, KVPair<Long, Long>> partitions = pdRestService.balancePartitions();
            return partitions;
        } catch (PDException e) {
            e.printStackTrace();
            return null;
        }
    }

    @GetMapping(value = "/splitPartitions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String splitPartitions() {
        try {
            List<Metapb.Partition> partitions = pdRestService.splitPartitions();
            return toJSON(partitions, "partitions");
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }

    @GetMapping(value = "/balanceLeaders", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String balanceLeaders() {
        try {
            Map<Integer, Long> leaders = pdRestService.balancePartitionLeader();
            return toJSON(leaders);
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }

    @GetMapping(value = "/compact")
    public String dbCompaction() throws PDException {
        pdRestService.dbCompaction();
        return "compact ok";
    }
}
