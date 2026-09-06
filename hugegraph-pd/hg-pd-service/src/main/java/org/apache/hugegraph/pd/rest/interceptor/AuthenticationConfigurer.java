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

package org.apache.hugegraph.pd.rest.interceptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class AuthenticationConfigurer implements WebMvcConfigurer {

    @Autowired
    RestAuthentication restAuthentication;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(restAuthentication)
                .addPathPatterns("/**")
                // /actuator/** states the intent for nested probe paths such as
                // /actuator/metrics/{name}, but it is not what keeps them open.
                // Actuator is served by WebMvcEndpointHandlerMapping, which only
                // picks up MappedInterceptor beans; one added through this
                // registry is never a bean, so it is attached to the MVC handler
                // mappings alone and never sees an actuator request either way.
                // What is reachable there is bounded by
                // management.endpoints.web.exposure.include.
                .excludePathPatterns("/actuator/**", "/v1/health", "/v1/prom/targets/*");
    }
}
