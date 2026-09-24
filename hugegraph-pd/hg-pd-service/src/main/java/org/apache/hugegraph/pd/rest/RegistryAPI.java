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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.GetMembersResponse;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.license.LicenseVerifierService;
import org.apache.hugegraph.pd.model.RegistryQueryRestRequest;
import org.apache.hugegraph.pd.model.RegistryRestRequest;
import org.apache.hugegraph.pd.model.RegistryRestResponse;
import org.apache.hugegraph.pd.rest.MemberAPI.CallStreamObserverWrap;
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
public class RegistryAPI extends API {

    @Autowired
    PDRestService pdRestService;
    @Autowired
    PDService pdService;

    /**
     * Register nodes with the registry center
     * <p>
     * Registers node information with the registry center via a POST request and returns the registration result.
     * The request's Content-Type is application/json, and the response's Content-Type is also application/json.
     *
     * @param body    The request body containing registration information, including application name, version, address, tags, and registration interval, etc.
     * @param request The HTTP request object used to obtain request-related information
     * @return Returns the response information from the registration center, including whether the registration was successful and any error messages.
     * @throws PDException If an exception occurs during registration (such as parameter errors), it is captured and handled, and the corresponding error message is returned.
     * @throws PDRuntimeException If an exception occurs during runtime (such as license verification failure), it is captured and handled, and the corresponding error message is returned.
     */
    @PostMapping(value = "/registry", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RegistryRestResponse register(@RequestBody RegistryRestRequest body,
                                         HttpServletRequest request) {
        RegistryRestResponse registryResponse = null;
        try {
            long interval = Long.valueOf(body.getInterval()).longValue();
            NodeInfo info = NodeInfo.newBuilder().setAppName(body.getAppName())
                                    .setVersion(body.getVersion())
                                    .setAddress(body.getAddress()).putAllLabels(body.getLabels())
                                    .setInterval(interval).build();
            registryResponse = pdRestService.register(info);
        } catch (PDException e) {
            registryResponse = new RegistryRestResponse();
            registryResponse.setErrorType(Pdpb.ErrorType.UNRECOGNIZED);
            registryResponse.setMessage(e.getMessage());
        } catch (PDRuntimeException e) {
            registryResponse = new RegistryRestResponse();
            registryResponse.setErrorType(Pdpb.ErrorType.LICENSE_VERIFY_ERROR);
            registryResponse.setMessage(e.getMessage());
        }
        return registryResponse;
    }

    /**
     * Get registration information
     * Get registration information that matches the query conditions via an HTTP POST request
     *
     * @param body    Request body containing query conditions, including application name, tags, version, and other information
     * @param request HTTP request object used to receive request-related information
     * @return Returns a response object containing registration information RegistryRestResponse
     * @throws Exception If an exception occurs during request processing, the exception will be caught and a warning log will be recorded, and the response object will contain error information
     */
    @PostMapping(value = "/registryInfo", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RegistryRestResponse getInfo(@RequestBody RegistryQueryRestRequest body,
                                        HttpServletRequest request) {
        RegistryRestResponse response = new RegistryRestResponse();
        try {
            boolean labelNotEmpty = body.getLabels() != null && !body.getLabels().isEmpty();
            Query query = Query.newBuilder()
                               .setAppName(StringUtils.isEmpty(body.getAppName()) ? "" :
                                           body.getAppName())
                               .putAllLabels(labelNotEmpty ? body.getLabels() : new HashMap<>())
                               .setVersion(StringUtils.isEmpty(body.getVersion()) ? "" :
                                           body.getVersion())
                               .build();
            ArrayList<RegistryRestRequest> registryResponse = pdRestService.getNodeInfo(query);
            response.setErrorType(Pdpb.ErrorType.OK);
            response.setData(registryResponse);
        } catch (Exception e) {
            log.warn(e.getMessage());
            response.setErrorType(Pdpb.ErrorType.UNRECOGNIZED);
            response.setMessage(e.getMessage());
        }
        return response;
    }

    /**
     * Retrieve all registration information
     * This interface retrieves all registration information via a GET request, including
     * standard registration details, PD member information, and Store member information.
     * It encapsulates this information within a RegistryRestResponse object for return.
     *
     * @param request HTTP request object
     * @return RegistryRestResponse object containing all registration information and response
     * data such as error types
     * @throws Exception If an exception occurs during request processing, it will be caught and
     * a warning log recorded, while the response error type will be set to UNRECOGNIZED
     */
    @GetMapping(value = "/allInfo", consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RegistryRestResponse allInfo(HttpServletRequest request) {
        RegistryRestResponse response = new RegistryRestResponse();
        try {
            //1.normal registry
            Query query =
                    Query.newBuilder().setAppName("").putAllLabels(new HashMap<>()).setVersion("")
                         .build();
            ArrayList<RegistryRestRequest> registryResponse = pdRestService.getNodeInfo(query);
            //2.pd member
            LinkedList<RegistryRestRequest> pdMembers = getMembers();
            //3.store member
            List<Metapb.Store> stores = pdRestService.getStores("");
            LinkedList<RegistryRestRequest> storeMembers = new LinkedList<>();
            for (Metapb.Store store : stores) {
                RegistryRestRequest restRequest = new RegistryRestRequest();
                restRequest.setAddress(store.getAddress());
                restRequest.setVersion(store.getVersion());
                restRequest.setAppName(STORE);
                restRequest.setId(String.valueOf(store.getId()));
                storeMembers.add(restRequest);
            }
            response.setErrorType(Pdpb.ErrorType.OK);
            HashMap<String, Serializable> result = new HashMap<>();
            result.put("other", registryResponse);
            result.put(PD, pdMembers);
            result.put(STORE, storeMembers);
            response.setData(result);
        } catch (Exception e) {
            log.warn(e.getMessage());
            response.setErrorType(Pdpb.ErrorType.UNRECOGNIZED);
            response.setMessage(e.getMessage());
        }
        return response;
    }

    private LinkedList<RegistryRestRequest> getMembers() throws Exception {
        CallStreamObserverWrap<GetMembersResponse> response = new CallStreamObserverWrap<>();
        pdService.getMembers(Pdpb.GetMembersRequest.newBuilder().build(), response);
        LinkedList<RegistryRestRequest> members = new LinkedList<>();
        List<Metapb.Member> membersList = response.get().get(0).getMembersList();
        for (Metapb.Member member : membersList) {
            RegistryRestRequest restRequest = new RegistryRestRequest();
            restRequest.setAddress(member.getRestUrl());
            restRequest.setVersion(VERSION);
            restRequest.setAppName(PD);
            members.add(restRequest);
        }
        return members;
    }

    /**
     * Retrieve licence information
     * Obtains the licence context information via an HTTP GET request and returns it
     * encapsulated within a response object.
     *
     * @param request HTTP request object
     * @return RegistryRestResponse Response object containing licence information.
     * If licence information is successfully retrieved, errorType is OK and the data field
     * contains the licence context;
     * If an exception occurs, errorType is UNRECOGNIZED and includes the exception message.
     * @throws Exception If an exception occurs while processing the request or retrieving
     * licence information, it is caught and a warning log is recorded.
     */
    @GetMapping(value = "/license", consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RegistryRestResponse getLicenseInfo(HttpServletRequest request) {
        RegistryRestResponse response = new RegistryRestResponse();
        try {
            response.setErrorType(Pdpb.ErrorType.OK);
            LicenseVerifierService licenseVerifierService = pdService.getLicenseVerifierService();
            response.setData(licenseVerifierService.getContext());
        } catch (Exception e) {
            log.warn(e.getMessage());
            response.setErrorType(Pdpb.ErrorType.UNRECOGNIZED);
            response.setMessage(e.getMessage());
        }
        return response;
    }

    /**
     * Retrieve Licence Machine Information
     * This interface obtains machine information related to the licence via a GET request,
     * returning the data in JSON format.
     *
     * @param request HTTP request object to receive client request information
     * @return RegistryRestResponse Response object containing licence machine information.
     * Returns machine details upon successful retrieval; otherwise returns error information.
     * @throws Exception If an exception occurs during request processing or licence machine
     * information retrieval, it will be caught and a warning log recorded, whilst returning a
     * response object containing exception details.
     */
    @GetMapping(value = "/license/machineInfo", consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RegistryRestResponse getLicenseMachineInfo(HttpServletRequest request) {
        RegistryRestResponse response = new RegistryRestResponse();
        try {
            response.setErrorType(Pdpb.ErrorType.OK);
            LicenseVerifierService licenseVerifierService = pdService.getLicenseVerifierService();
            response.setData(licenseVerifierService.getIpAndMac());
        } catch (Exception e) {
            log.warn(e.getMessage());
            response.setErrorType(Pdpb.ErrorType.UNRECOGNIZED);
            response.setMessage(e.getMessage());
        }
        return response;
    }
}
