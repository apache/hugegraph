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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.hugegraph.pd.model.SDConfig;
import org.apache.hugegraph.pd.service.SDConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;
@RestController
@Slf4j
@RequestMapping("/v1/prom")
public class SDConfigAPI {

    @Autowired
    private SDConfigService service;

    /**
     * Get Prometheus monitoring targets based on application name
     * Use a GET request to get a list of corresponding Prometheus monitoring targets based on
     * the provided application name
     * The URL path is: /targets/{appName}, and the response data type is JSON
     *
     * @param appName Application name, this parameter is a path variable and is required
     * @return ResponseEntity object containing the JSON-formatted response of the Prometheus
     * monitoring target list
     */
    @GetMapping(value = "/targets/{appName}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<SDConfig>> getPromTargets(
            @PathVariable(value = "appName", required = true) String appName) {
        return ResponseEntity.of(Optional.ofNullable(this.service.getTargets(appName)));
    }

    /**
     * Get all target configuration interfaces.
     * Get a list of all target configurations via a GET request and return it in JSON format.
     *
     * @return ResponseEntity encapsulated List<SDConfig> object containing all target configurations.
     */
    @GetMapping(value = "/targets-all", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<SDConfig>> getPromAllTargets() {
        return ResponseEntity.of(Optional.ofNullable(this.service.getAllTargets()));
    }

    /**
     * Get sample monitoring targets based on application name
     * Based on the input application name (targetType), return the corresponding list of monitoring target configurations.
     * Supported application types are “node”, ‘store’, and “pd”, which correspond to different monitoring target configurations.
     * If the input application name is invalid, returns a list containing empty SDConfig objects.
     *
     * @param targetType Application type, supporting “node”, ‘store’, and “pd” types
     * @return A list of SDConfig objects containing monitoring targets. If targetType is an invalid type, returns a list containing empty SDConfig objects
     */
    @GetMapping(value = "/demo/targets/{appName}", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<SDConfig> getDemoTargets(
            @PathVariable(value = "appName", required = true) String targetType) {

        SDConfig model = null;
        switch (targetType) {
            case "node":
                model = SDConfig.of()
                                .addTarget("10.14.139.26:8100")
                                .addTarget("10.14.139.27:8100")
                                .addTarget("10.14.139.28:8100")
                                .setMetricsPath("/metrics")
                                .setScheme("http");
                break;
            case "store":
                model = SDConfig.of()
                                .addTarget("172.20.94.98:8521")
                                .addTarget("172.20.94.98:8522")
                                .addTarget("172.20.94.98:8523")
                                .setMetricsPath("/actuator/prometheus")
                                .setScheme("http");
                break;
            case "pd":
                model = SDConfig.of()
                                .addTarget("172.20.94.98:8620")
                                .setMetricsPath("/actuator/prometheus");

                break;
            default:
        }
        return model == null ? Collections.emptyList() : Collections.singletonList(model);
    }

    /**
     * Get service discovery configuration
     * Get service discovery configuration information based on application name and path
     *
     * @param appName Application name, request parameter, used to specify the application for which to get the configuration
     * @param path Optional parameter, request parameter, specifies the path for which to get the service discovery configuration
     * @return ResponseEntity object, contains a list of service discovery configurations, returned in JSON format
     */
    @GetMapping(value = "/sd_config", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<SDConfig>> getSDConfig(@RequestParam(value = "appName") String appName,
                                                      @RequestParam(value = "path", required = false)
                                                              String path) {
        return ResponseEntity.of(Optional.ofNullable(this.service.getConfigs(appName, path)));
    }

}
