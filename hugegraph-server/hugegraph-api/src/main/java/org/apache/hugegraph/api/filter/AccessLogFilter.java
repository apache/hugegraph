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

package org.apache.hugegraph.api.filter;

import static org.apache.hugegraph.api.filter.PathFilter.REQUEST_TIME;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_FAILED_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_RESPONSE_TIME_HISTOGRAM;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_SUCCESS_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_TOTAL_COUNTER;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Map;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.DecompressInterceptor.Decompress;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.util.Log;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;

import jakarta.inject.Singleton;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.Provider;

@Provider
@Singleton
public class AccessLogFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final Logger LOG = Log.logger(AccessLogFilter.class);

    private static final String DELIMITER = "/";
    private static final String GRAPHS = "graphs";
    private static final String GREMLIN = "gremlin";
    private static final String CYPHER = "cypher";

    // Request property holding the bounded request body preview for the slow query log
    public static final String REQUEST_BODY = "request_body";
    public static final String UNKNOWN_IP = "<unknown_ip>";

    private static final String TRUNCATED_MARK = "...";
    private static final String ENCODED_BODY = "<encoded>";
    private static final Charset DEFAULT_CHARSET = Charset.forName(API.CHARSET);

    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;

    @Context
    private jakarta.inject.Provider<GraphManager> managerProvider;

    @Context
    private jakarta.inject.Provider<Request> requestProvider;

    @Context
    private ResourceInfo resourceInfo;

    public static boolean needRecordLog(ContainerRequestContext context) {
        String path = context.getUriInfo().getPath();

        // GraphsAPI/CypherAPI/Job GremlinAPI
        if (path.startsWith(GRAPHS)) {
            if (HttpMethod.GET.equals(context.getMethod()) || path.endsWith(CYPHER)) {
                return true;
            }
        }
        // Direct GremlinAPI
        return path.endsWith(GREMLIN);
    }

    private String join(String path1, String path2) {
        return String.join(DELIMITER, path1, path2);
    }

    private static String normalizePath(ContainerRequestContext requestContext) {
        // Replace variable parts of the path with placeholders
        String requestPath = requestContext.getUriInfo().getPath();
        // get uri params
        MultivaluedMap<String, String> pathParameters = requestContext.getUriInfo().getPathParameters();

        String newPath = requestPath;
        for (Map.Entry<String, java.util.List<String>> entry : pathParameters.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().get(0);
            if ("graph".equals(key)) {
                newPath = newPath.replace(key, value);
            }
            newPath = newPath.replace(value, key);
        }

        LOG.trace("normalize path, original path: '{}', new path: '{}'", requestPath, newPath);
        return newPath;
    }

    /**
     * Keep a bounded preview of the request body for the slow query log.
     * Only the first {@link ServerOptions#SLOW_QUERY_LOG_BODY_LIMIT} bytes are read, and they are replayed in front
     * of the untouched remainder of the entity stream, so the resource method still receives the whole body.
     *
     * @param requestContext requestContext
     */
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (!mayHaveBody(requestContext.getMethod()) || !needRecordLog(requestContext)) {
            return;
        }

        HugeConfig config = this.configProvider.get();
        long timeThreshold = config.get(ServerOptions.SLOW_QUERY_LOG_TIME_THRESHOLD);
        int bodyLimit = config.get(ServerOptions.SLOW_QUERY_LOG_BODY_LIMIT);
        if (timeThreshold <= 0 || bodyLimit <= 0) {
            return;
        }

        if (this.decodesEntity()) {
            // The resource decodes its entity later (DecompressInterceptor), so the raw bytes here are not readable
            requestContext.setProperty(REQUEST_BODY, ENCODED_BODY);
            return;
        }

        InputStream entity = requestContext.getEntityStream();
        // Read one byte past the limit to know whether the preview is truncated
        byte[] prefix = new byte[bodyLimit + 1];
        int length = entity.readNBytes(prefix, 0, prefix.length);
        requestContext.setEntityStream(new SequenceInputStream(new ByteArrayInputStream(prefix, 0, length), entity));
        Charset charset = requestCharset(requestContext);
        requestContext.setProperty(REQUEST_BODY, preview(prefix, length, bodyLimit, charset));
    }

    /**
     * Use filter to log request info
     *
     * @param requestContext  requestContext
     * @param responseContext responseContext
     */
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {
        // Grab corresponding request / response info from context;
        URI uri = requestContext.getUriInfo().getRequestUri();
        String method = requestContext.getMethod();
        String path = normalizePath(requestContext);
        String metricsName = join(path, method);

        int status = responseContext.getStatus();
        if (isStatusXx(status)) {
            MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_TOTAL_COUNTER)).inc();
        }

        if (statusOk(responseContext.getStatus())) {
            MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_SUCCESS_COUNTER)).inc();
        } else {
            // TODO: The return codes for compatibility need to be further detailed.
            LOG.trace("Failed Status: {}", status);
            if (status != 500 && status != 415) {
                MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_FAILED_COUNTER)).inc();
            }
        }

        Object requestTime = requestContext.getProperty(REQUEST_TIME);
        if (requestTime != null) {
            long now = System.currentTimeMillis();
            long start = (Long) requestTime;
            long executeTime = now - start;

            if (status != 500 && status != 415) {
                MetricsUtil.registerHistogram(join(metricsName,
                                                   METRICS_PATH_RESPONSE_TIME_HISTOGRAM))
                           .update(executeTime);
            }

            HugeConfig config = configProvider.get();
            long timeThreshold = config.get(ServerOptions.SLOW_QUERY_LOG_TIME_THRESHOLD);
            // Record slow query if meet needs, watch out the perf
            if (timeThreshold > 0 && executeTime > timeThreshold && needRecordLog(requestContext)) {
                String clientIp = this.clientIp();
                Object body = requestContext.getProperty(REQUEST_BODY);
                LOG.info("[Slow Query] ip={}, execTime={}ms, method={}, path={}, query={}, " +
                         "body={}", clientIp, executeTime, method, singleLine(path),
                         singleLine(uri.getQuery()), singleLine(body));
            }
        }

        // Unset the context in "HugeAuthenticator", need distinguish Graph/Auth server lifecycle
        GraphManager manager = managerProvider.get();
        // TODO: transfer Authorizer if we need after.
        if (manager.requireAuthentication()) {
            manager.unauthorized(requestContext.getSecurityContext());
        }
    }

    private static boolean mayHaveBody(String method) {
        // DELETE endpoints take path/query params only, so there is no body to record
        return HttpMethod.POST.equals(method) || HttpMethod.PUT.equals(method);
    }

    private boolean decodesEntity() {
        Method method = this.resourceInfo == null ? null : this.resourceInfo.getResourceMethod();
        return method != null && method.isAnnotationPresent(Decompress.class);
    }

    private static Charset requestCharset(ContainerRequestContext requestContext) {
        MediaType mediaType = requestContext.getMediaType();
        if (mediaType == null) {
            return DEFAULT_CHARSET;
        }
        String charset = mediaType.getParameters().get(MediaType.CHARSET_PARAMETER);
        return charset == null ? DEFAULT_CHARSET : Charset.forName(charset);
    }

    private static String preview(byte[] bytes, int length, int limit, Charset charset) {
        boolean truncated = length > limit;
        int size = Math.min(length, limit);
        CharsetDecoder decoder = charset.newDecoder().onMalformedInput(CodingErrorAction.REPLACE)
                                        .onUnmappableCharacter(CodingErrorAction.REPLACE);
        CharBuffer chars = CharBuffer.allocate((int) (size * decoder.maxCharsPerByte()) + 1);
        /*
         * A multi-byte character cut by the limit is dropped rather than turned
         * into a replacement character: endOfInput=false keeps the incomplete
         * tail undecoded
         */
        decoder.decode(ByteBuffer.wrap(bytes, 0, size), chars, !truncated);
        if (!truncated) {
            decoder.flush(chars);
        }
        String body = chars.flip().toString();
        return truncated ? body + TRUNCATED_MARK : body;
    }

    private static String singleLine(Object value) {
        // Path, query and body are client controlled, keep the log entry on a single line
        return value == null ? null : value.toString().replace("\r", "\\r").replace("\n", "\\n");
    }

    private String clientIp() {
        Request request = this.requestProvider.get();
        String address = request == null ? null : request.getRemoteAddr();
        return address == null || address.isEmpty() ? UNKNOWN_IP : address;
    }

    private boolean statusOk(int status) {
        return status >= 200 && status < 300;
    }

    private boolean isStatusXx(int status) {
        return status != 500 && status != 415;
    }
}
