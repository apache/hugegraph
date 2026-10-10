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

package org.apache.hugegraph.server;

import java.util.Map;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;

@ChannelHandler.Sharable
public class HttpGremlinRequestHandler extends HttpGremlinEndpointHandler {

    public HttpGremlinRequestHandler(Map<String, MessageSerializer<?>> serializers,
                                     GremlinExecutor executor,
                                     GraphManager graphManager,
                                     Settings settings) {
        super(serializers, executor, graphManager, settings);
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (!(message instanceof FullHttpRequest)) {
            super.channelRead(context, message);
            return;
        }

        FullHttpRequest request = (FullHttpRequest) message;
        try {
            /*
             * TinkerPop 3.8.1 releases the request before resolving aliases,
             * then releases it again on an unknown alias. That second release
             * writes a spurious 500 after the intended 400 on the same stream.
             * Own the original reference here and let the upstream handler
             * parse a view whose release is a no-op. It parses synchronously;
             * asynchronous evaluation holds only the parsed RequestMessage.
             */
            FullHttpRequest view = request.replace(
                    Unpooled.unreleasableBuffer(request.content()));
            super.channelRead(context, view);
        } finally {
            request.release();
        }
    }
}
