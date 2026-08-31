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

package org.apache.hugegraph;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.junit.Assert;
import org.junit.Test;

public class SchemaDriverTest {

    @Test
    public void testDestroyClosesOwnedKvClient() throws Exception {
        TrackingKvClient client = new TrackingKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        instanceReference().set(driver);

        try {
            SchemaDriver.destroy();

            Assert.assertTrue(client.closed);
            Assert.assertNull(SchemaDriver.getInstance());
        } finally {
            instanceReference().set(null);
            client.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<SchemaDriver> instanceReference() throws Exception {
        Field field = SchemaDriver.class.getDeclaredField("INSTANCE");
        field.setAccessible(true);
        return (AtomicReference<SchemaDriver>) field.get(null);
    }

    private static class TrackingKvClient extends KvClient<WatchResponse> {

        private boolean closed;

        TrackingKvClient() {
            super(PDConfig.of("127.0.0.1:8686"));
        }

        @Override
        public void listen(String key, Consumer<WatchResponse> consumer) throws PDException {
            // Avoid opening a real PD stream while constructing SchemaDriver.
        }

        @Override
        public void close() {
            this.closed = true;
            super.close();
        }
    }
}
