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

package org.apache.hugegraph.api.profile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.version.CoreVersion;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.glassfish.jersey.model.Parameter.Source;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.server.model.ResourceMethod;

import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.UriInfo;

@Path("/")
@Singleton
@Tag(name = "ProfileAPI")
public class ProfileAPI {

    private static final String SERVICE = "hugegraph";
    private static final String DOC = "https://hugegraph.apache.org/docs/";
    private static final String API_DOC = DOC + "clients/";
    private static final String SWAGGER_UI = "/swagger-ui/index.html";

    private static String SERVER_PROFILES = null;
    private static final Map<String, String> API_PROFILES = new ConcurrentHashMap<>();

    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public String getProfile(@Context HugeConfig conf, @Context Application application) {
        // May init multi times by multi threads, but no effect on the results
        if (SERVER_PROFILES != null) {
            return SERVER_PROFILES;
        }

        Map<String, Object> profiles = InsertionOrderUtil.newMap();
        profiles.put("service", SERVICE);
        profiles.put("version", CoreVersion.VERSION.toString());
        profiles.put("doc", DOC);
        profiles.put("api_doc", API_DOC);
        profiles.put("swagger_ui", conf.get(ServerOptions.REST_SERVER_URL) + SWAGGER_UI);
        Set<String> apis = new TreeSet<>();
        for (Class<?> clazz : application.getClasses()) {
            if (!isAnnotatedPathClass(clazz)) {
                continue;
            }
            Resource resource = Resource.from(clazz);
            APICategory apiCategory = APICategory.parse(resource.getName());
            apis.add(apiCategory.dir);
        }
        profiles.put("apis", apis);
        SERVER_PROFILES = JsonUtil.toJson(profiles);
        return SERVER_PROFILES;
    }

    @GET
    @Path("apis")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public String showAllAPIs(@Context Application application,
                              @Context UriInfo uriInfo) {
        String basePath = "/" + StringUtils.strip(uriInfo.getBaseUri().getPath(), "/");
        return API_PROFILES.computeIfAbsent(basePath,
                                            path -> buildAPIProfiles(application, path));
    }

    private static String buildAPIProfiles(Application application, String basePath) {
        APIProfiles apiProfiles = new APIProfiles();
        for (Class<?> clazz : application.getClasses()) {
            if (!isAnnotatedPathClass(clazz)) {
                continue;
            }

            Resource resource = Resource.from(clazz);
            APICategory apiCategory = APICategory.parse(resource.getName());

            // Include the REST application path, which may not be the origin root.
            String url = StringUtils.stripEnd(basePath, "/") + "/" +
                         StringUtils.strip(resource.getPath(), "/");
            // List all methods of this resource
            for (ResourceMethod rm : resource.getResourceMethods()) {
                APIProfile profile = APIProfile.parse(url, rm);
                apiProfiles.put(apiCategory, profile);
            }
            // List all methods of this resource's child resources
            for (Resource childResource : resource.getChildResources()) {
                String childUrl = StringUtils.stripEnd(url, "/") + "/" +
                                  StringUtils.stripStart(childResource.getPath(), "/");
                for (ResourceMethod rm : childResource.getResourceMethods()) {
                    APIProfile profile = APIProfile.parse(childUrl, rm);
                    apiProfiles.put(apiCategory, profile);
                }
            }
        }
        return JsonUtil.toJson(apiProfiles);
    }

    private static boolean isAnnotatedPathClass(Class<?> clazz) {
        if (clazz.isAnnotationPresent(Path.class)) {
            return true;
        }
        for (Class<?> i : clazz.getInterfaces()) {
            if (i.isAnnotationPresent(Path.class)) {
                return true;
            }
        }
        return false;
    }

    private static class APIProfiles {

        @JsonProperty("apis")
        private final Map<String, Map<String, List<APIProfile>>> apis;

        public APIProfiles() {
            this.apis = new TreeMap<>();
        }

        public void put(APICategory category, APIProfile profile) {
            Map<String, List<APIProfile>> categories;
            categories = this.apis.computeIfAbsent(category.dir,
                                                   k -> new TreeMap<>());
            List<APIProfile> profiles = categories.computeIfAbsent(
                    category.category,
                    k -> new ArrayList<>());
            profiles.add(profile);
        }
    }

    private static class APIProfile {

        @JsonProperty("url")
        private final String url;
        @JsonProperty("method")
        private final String method;
        @JsonProperty("parameters")
        private final List<ParamInfo> parameters;

        public APIProfile(String url, String method,
                          List<ParamInfo> parameters) {
            this.url = url;
            this.method = method;
            this.parameters = parameters;
        }

        public static APIProfile parse(String url, ResourceMethod resource) {
            String method = resource.getHttpMethod();
            List<ParamInfo> params = new ArrayList<>();
            for (Parameter param : resource.getInvocable().getParameters()) {
                if (param.getSource() == Source.QUERY) {
                    String name = param.getSourceName();
                    String type = param.getType().getTypeName();
                    String defaultValue = param.getDefaultValue();
                    params.add(new ParamInfo(name, type, defaultValue));
                } else if (param.getSource() == Source.ENTITY) {
                    String type = param.getType().getTypeName();
                    params.add(new ParamInfo("body", type));
                }
            }
            return new APIProfile(url, method, params);
        }

        private static class ParamInfo {

            @JsonProperty("name")
            private final String name;
            @JsonProperty("type")
            private final String type;
            @JsonProperty("default_value")
            private final String defaultValue;

            public ParamInfo(String name, String type) {
                this(name, type, null);
            }

            public ParamInfo(String name, String type, String defaultValue) {
                this.name = name;
                this.type = type;
                this.defaultValue = defaultValue;
            }
        }
    }

    private static class APICategory {

        private final String dir;
        private final String category;

        public APICategory(String dir, String category) {
            this.dir = dir;
            this.category = category;
        }

        public static APICategory parse(String fullName) {
            String[] parts = StringUtils.split(fullName, ".");
            E.checkState(parts.length >= 2, "Invalid api name");
            String dir = parts[parts.length - 2];
            String category = parts[parts.length - 1];
            return new APICategory(dir, category);
        }
    }
}
