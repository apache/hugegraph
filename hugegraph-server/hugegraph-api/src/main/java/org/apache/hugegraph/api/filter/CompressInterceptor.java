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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.text.ParseException;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.hugegraph.api.filter.CompressInterceptor.Compress;
import org.apache.hugegraph.util.Log;
import org.glassfish.jersey.message.internal.AcceptableToken;
import org.glassfish.jersey.message.internal.HttpHeaderReader;
import org.slf4j.Logger;

import jakarta.inject.Singleton;
import jakarta.ws.rs.NameBinding;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.Provider;
import jakarta.ws.rs.ext.WriterInterceptor;
import jakarta.ws.rs.ext.WriterInterceptorContext;

@Provider
@Singleton
@Compress
public class CompressInterceptor implements WriterInterceptor {

    public static final String GZIP = "gzip";

    private static final Logger LOG = Log.logger(CompressInterceptor.class);

    private static final Pattern TRAILING_DOT_QUALITY =
            Pattern.compile("(;[ \t]*[qQ]=[01])\\.(?=[ \t]*(?:[,;]|\\z))");

    // Set compress output buffer size to 4KB (about 40~600 vertices)
    public static final int BUFFER_SIZE = 1024 * 4;

    @Context
    private jakarta.inject.Provider<HttpHeaders> requestHeaders;

    @Override
    public void aroundWriteTo(WriterInterceptorContext context)
            throws IOException, WebApplicationException {
        // Exceptions and other responses without @Compress remain unchanged.
        Compress compression = getCompressAnnotation(context);
        if (compression != null && !context.getHeaders().containsKey(HttpHeaders.CONTENT_ENCODING)) {
            addVaryHeader(context.getHeaders());
        }
        if (compression != null &&
            !context.getHeaders().containsKey(HttpHeaders.CONTENT_ENCODING) &&
            this.acceptsGzip()) {
            try {
                this.compress(context, compression);
            } catch (Throwable e) {
                LOG.warn("Failed to compress response", e);
                /*
                 * FIXME: This will cause java.lang.IllegalStateException:
                 *  Illegal attempt to call getOutputStream() after getWriter()
                 */
                throw e;
            }
        }

        context.proceed();
    }

    private boolean acceptsGzip() {
        String accept = this.requestHeaders.get().getHeaderString(HttpHeaders.ACCEPT_ENCODING);
        if (accept == null || accept.isEmpty()) {
            return false;
        }
        // Jersey 3.0.3 misreads legal q=0. and q=1.; retain its token/quality validation.
        accept = TRAILING_DOT_QUALITY.matcher(accept).replaceAll("$1");
        int gzip = -1;
        int wildcard = 0;
        int identity = 0;
        try {
            // Jersey parses HTTP tokens and quality factors; explicit encodings override '*'.
            for (AcceptableToken token : HttpHeaderReader.readAcceptToken(accept)) {
                if (GZIP.equalsIgnoreCase(token.getToken())) {
                    gzip = token.getQuality();
                } else if ("*".equals(token.getToken())) {
                    wildcard = token.getQuality();
                } else if ("identity".equalsIgnoreCase(token.getToken())) {
                    identity = token.getQuality();
                }
            }
        } catch (ParseException e) {
            return false;
        }
        int quality = gzip >= 0 ? gzip : wildcard;
        // If no encoding is acceptable, retain the plain response (RFC 9110, 12.4.1).
        // Rejecting it here could hide a POST that has already changed graph data.
        return quality > 0 && quality >= identity;
    }

    private static void addVaryHeader(MultivaluedMap<String, Object> headers) {
        for (Object value : headers.getOrDefault(HttpHeaders.VARY, java.util.Collections.emptyList())) {
            for (String field : value.toString().split(",")) {
                if ("*".equals(field.trim()) || HttpHeaders.ACCEPT_ENCODING.equalsIgnoreCase(field.trim())) {
                    return;
                }
            }
        }
        headers.add(HttpHeaders.VARY, HttpHeaders.ACCEPT_ENCODING);
    }

    private void compress(WriterInterceptorContext context, Compress compression)
            throws IOException {
        final String encoding = compression.value();
        final int buffer = compression.buffer();

        // Update header
        MultivaluedMap<String, Object> headers = context.getHeaders();
        headers.remove("Content-Length");
        headers.add("Content-Encoding", encoding);

        // Replace output stream with new compression stream
        OutputStream output = null;
        if (encoding.equalsIgnoreCase(GZIP)) {
            output = new GZIPOutputStream(context.getOutputStream(), buffer);
        } else {
            // NOTE: Currently we just support GZIP.
            throw new WebApplicationException("Can't support: " + encoding);
        }
        context.setOutputStream(output);
    }

    private static Compress getCompressAnnotation(WriterInterceptorContext c) {
        for (Annotation annotation : c.getAnnotations()) {
            if (annotation.annotationType() == Compress.class) {
                return (Compress) annotation;
            }
        }
        return null;
    }

    @NameBinding
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Compress {

        String value() default GZIP;

        int buffer() default BUFFER_SIZE;
    }
}
