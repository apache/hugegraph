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

import java.io.Serializable;

import org.apache.hugegraph.store.grpc.state.ScanState;
import org.apache.hugegraph.store.node.entry.RestResult;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeState;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.model.HgNodeStatus;
import org.apache.hugegraph.store.node.task.TTLCleaner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.protobuf.util.JsonFormat;

/**
 * created on 2021/11/1
 */
@RestController
public class HgStoreStatusController {

    @Autowired
    HgStoreStreamImpl streamImpl;
    @Autowired
    TTLCleaner cleaner;

    @GetMapping("/-/echo")
    public HgNodeStatus greeting(
            @RequestParam(value = "name", defaultValue = "World") String name) {
        return new HgNodeStatus(0, name + " is ok.");
    }

    @GetMapping("/-/state")
    public HgNodeStatus getState() {
        return new HgNodeStatus(0, HgStoreNodeState.getState().name());
    }

    @PutMapping("/-/state")
    public HgNodeStatus setState(@RequestParam(value = "name") String name) {

        switch (name) {
            case "starting":
                HgStoreNodeState.goStarting();
                break;
            case "online":
                HgStoreNodeState.goOnline();
                break;
            case "stopping":
                HgStoreNodeState.goStopping();
                break;
            default:
                return new HgNodeStatus(1000, "invalid parameter: " + name);
        }

        return new HgNodeStatus(0, name);
    }

    @GetMapping(value = "/-/scan",
                produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Serializable getScanState() {
        RestResult result = new RestResult();
        try {
            ScanState state = streamImpl.getState();
            JsonFormat.Printer printer = JsonFormat.printer();
            printer = printer.includingDefaultValueFields().preservingProtoFieldNames();
            return printer.print(state);
        } catch (Exception e) {
            result.setState(RestResult.ERR);
            result.setMessage(e.getMessage());
            return result;
        }
    }

    @GetMapping(value = "/-/cleaner",
                produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Serializable ttlClean() {
        RestResult result = new RestResult();
        try {
            cleaner.submit();
            result.setState(RestResult.OK);
            result.setMessage("");
            return result;
        } catch (Exception e) {
            result.setState(RestResult.ERR);
            result.setMessage(e.getMessage());
            return result;
        }
    }

    @GetMapping(value = "/v1/health", produces = MediaType.TEXT_PLAIN_VALUE)
    public Serializable checkHealthy() {
        return "";
    }

}
