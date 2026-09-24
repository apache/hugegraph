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

import javax.servlet.http.HttpServletRequest;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.node.entry.PartitionRequest;
import org.apache.hugegraph.store.node.entry.RestResult;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.alipay.sofa.jraft.option.RpcOptions;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/raft")
public class RaftAPI {

    @PostMapping(value = "/options", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestResult options(@RequestBody PartitionRequest body, HttpServletRequest request) {
        RestResult result = new RestResult();
        try {
            if (body.getId() == null) {
                result.setState(RestResult.ERR);
                result.setMessage("partition id could not be null");
                return result;
            }
            PartitionEngine pe =
                    HgStoreEngine.getInstance().getPartitionEngine(body.getId());
            if (pe == null) {
                result.setState(RestResult.ERR);
                result.setMessage("partition engine is null!");
                return result;
            }
            RpcOptions options = pe.getRaftGroupService().getNodeOptions();
            result.setData(options.toString());
            result.setState(RestResult.OK);
        } catch (Exception e) {
            result.setState(RestResult.ERR);
            result.setMessage(e.getMessage());
        }
        return result;
    }
}

