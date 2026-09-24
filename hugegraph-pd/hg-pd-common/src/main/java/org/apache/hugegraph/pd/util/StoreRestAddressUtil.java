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

package org.apache.hugegraph.pd.util;

import java.net.URI;
import java.util.Optional;

import org.apache.hugegraph.pd.grpc.Metapb;

public final class StoreRestAddressUtil {

    private static final String REST_PORT_LABEL = "rest.port";

    private StoreRestAddressUtil() {
    }

    public static String getRestAddress(Metapb.Store store) {
        if (store == null || store.getAddress().isEmpty()) {
            return null;
        }

        Optional<String> label = store.getLabelsList().stream()
                                      .filter(item -> REST_PORT_LABEL.equals(
                                              item.getKey()))
                                      .map(Metapb.StoreLabel::getValue)
                                      .findFirst();
        if (!label.isPresent()) {
            return null;
        }

        int port;
        try {
            port = Integer.parseInt(label.get().trim());
        } catch (NumberFormatException ignored) {
            return null;
        }
        if (port < 1 || port > 65535) {
            return null;
        }

        try {
            String address = store.getAddress().trim();
            URI uri = address.contains("://") ? URI.create(address) :
                      URI.create("http://" + address);
            String host = uri.getHost();
            if (host == null || host.isEmpty()) {
                return null;
            }
            String hostPart = host.contains(":") && !host.startsWith("[") ?
                              "[" + host + "]" : host;
            return hostPart + ":" + port;
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }
}
