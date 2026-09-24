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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.model.GraphRestRequest;
import org.apache.hugegraph.pd.model.GraphStatistics;
import org.apache.hugegraph.pd.model.RestApiResponse;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.service.PDService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/v1")
public class GraphAPI extends API {

    @Autowired
    PDRestService pdRestService;
    @Autowired
    PDService pdService;

    /**
     * Get partition size range
     * <p>
     * This interface is used to obtain the minimum and maximum values of partition sizes in the current system.
     *
     * @return RestApiResponse object containing the partition size range
     * @throws PDException If an exception occurs while obtaining the partition size range, a PDException exception is thrown.
     */
    @GetMapping(value = "/graph/partitionSizeRange", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getPartitionSizeRange() {
        try {
            int minPartitionSize = 1;
            int maxPartitionSize = pdService.getStoreNodeService().getShardGroups().size();
            Map<String, Integer> dataMap = new HashMap<>();
            dataMap.put("minPartitionSize", minPartitionSize);
            dataMap.put("maxPartitionSize", maxPartitionSize);
            return new RestApiResponse(dataMap, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            log.error("PDException:", e);
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
    }

    /**
     * Get all graph information
     * This interface uses a GET request to obtain all graph information and filters out graphs whose names end with “/g”.
     * The information of these graphs is encapsulated in a RestApiResponse object and returned.
     *
     * @return A RestApiResponse object containing the filtered graph information
     * The returned object includes a “graphs” field, whose value is a list containing GraphStatistics objects
     * @throws PDException If an exception occurs while retrieving graph information, a PDException exception is thrown
     */
    @GetMapping(value = "/graphs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getGraphs() {
        RestApiResponse response = new RestApiResponse();
        try {
            List<Metapb.Graph> graphs = pdRestService.getGraphs();
            List<GraphStatistics> resultGraphs = new ArrayList<>();
            for (Metapb.Graph graph : graphs) {
                if ((graph.getGraphName() != null) && (graph.getGraphName().endsWith("/g"))) {
                    resultGraphs.add(new GraphStatistics(graph, pdRestService, pdService));
                }
            }
            HashMap<String, Object> dataMap = new HashMap<>();
            dataMap.put("graphs", resultGraphs);
            response.setData(dataMap);
            response.setStatus(Pdpb.ErrorType.OK.getNumber());
            response.setMessage(Pdpb.ErrorType.OK.name());

        } catch (PDException e) {
            log.error("PDException: ", e);
            response.setData(new HashMap<String, Object>());
            response.setStatus(e.getErrorCode());
            response.setMessage(e.getMessage());
        }
        return response;
    }

    /**
     * Set graph information
     * <p>
     * Receive a GraphRestRequest object via an HTTP POST request, parse the graph name from the request URL,
     * and use the pdRestService service to obtain the current graph information.
     * If the current graph does not exist, create a new graph object;
     * if it exists, update the current graph object information (such as the number of partitions).
     * Finally, use the pdRestService service to update the graph information and return the updated graph information in JSON format.
     *
     * @param body GraphRestRequest object containing graph information
     * @param request HTTP request object used to obtain the graph name from the request URL
     * @return A JSON string containing the updated graph information
     * @throws PDException If a PD exception occurs while retrieving or updating the graph information, a PDException exception is thrown
     * @throws Exception If other exceptions occur while processing the request, an Exception exception is thrown
     */
    @PostMapping(value = "/graph/**", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String setGraph(@RequestBody GraphRestRequest body, HttpServletRequest request) {
        try {
            String requestURL = request.getRequestURL().toString();
            final String prefix = "/graph/";
            final int limit = 2;
            String graphName = requestURL.split(prefix, limit)[1];
            graphName = URLDecoder.decode(graphName, StandardCharsets.UTF_8);
            Metapb.Graph curGraph = pdRestService.getGraph(graphName);
            Metapb.Graph.Builder builder = Metapb.Graph.newBuilder(
                    curGraph == null ? Metapb.Graph.getDefaultInstance() : curGraph);
            builder.setGraphName(graphName);
            if (body.getPartitionCount() > 0) {
                builder.setPartitionCount(body.getPartitionCount());
            }

            Metapb.Graph newGraph = pdRestService.updateGraph(builder.build());
            return toJSON(newGraph, "graph");
        } catch (PDException exception) {
            return toJSON(exception);
        } catch (Exception e) {
            return toJSON(e);
        }
    }

    /**
     * Get graph information
     * <p>
     * Retrieves information about a specified graph via an HTTP GET request and returns it in JSON format.
     *
     * @param request HTTP request object used to retrieve the graph name from the request URL
     * @return RestApiResponse object containing graph information
     * @throws UnsupportedEncodingException Thrown if an unsupported encoding exception occurs during URL decoding
     */
    @GetMapping(value = "/graph/**", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getGraph(HttpServletRequest request) throws
                                                                UnsupportedEncodingException {
        RestApiResponse response = new RestApiResponse();
        GraphStatistics statistics;
        String requestURL = request.getRequestURL().toString();
        final String prefix = "/graph/";
        final int limit = 2;
        String graphName = requestURL.split(prefix, limit)[1];
        graphName = URLDecoder.decode(graphName, StandardCharsets.UTF_8);
        try {
            Metapb.Graph graph = pdRestService.getGraph(graphName);
            if (graph != null) {
                statistics = new GraphStatistics(graph, pdRestService, pdService);
                response.setData(statistics);
            } else {
                response.setData(new HashMap<String, Object>());
            }
            response.setStatus(Pdpb.ErrorType.OK.getNumber());
            response.setMessage(Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            log.error(e.getMessage());
            response.setData(new HashMap<String, Object>());
            response.setStatus(Pdpb.ErrorType.UNKNOWN.getNumber());
            response.setMessage(e.getMessage());
        }
        return response;
    }
}
