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

package org.apache.hugegraph.io;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;

/**
 * Legacy Tree response shape for the untyped GraphSON V1 message serializer.
 * Standard graph IO and typed message serializers use HugeGraphIoRegistry.
 */
public final class HugeGraphSONV1MessageIoRegistry extends AbstractIoRegistry {

    private static final HugeGraphSONV1MessageIoRegistry INSTANCE =
            new HugeGraphSONV1MessageIoRegistry();

    public static HugeGraphSONV1MessageIoRegistry instance() {
        return INSTANCE;
    }

    private HugeGraphSONV1MessageIoRegistry() {
        SimpleModule module = new SimpleModule("hugegraph-v1-message");
        HugeGraphSONModule.registerLegacyTreeSerializer(module);
        this.register(GraphSONIo.class, null, module);
    }
}
