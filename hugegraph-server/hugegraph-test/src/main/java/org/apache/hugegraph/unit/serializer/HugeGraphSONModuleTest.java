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

package org.apache.hugegraph.unit.serializer;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hugegraph.io.HugeGraphIoRegistry;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * The module is registered into the gremlin-server GraphSON mappers through
 * HugeGraphIoRegistry (gremlin-server.yaml: ioRegistries). The typed
 * mappers (v2/v3) call serializeWithType(), so every serializer added by
 * the module has to implement it or Gremlin results of that type fail.
 */
public class HugeGraphSONModuleTest extends BaseUnitTest {

    private static final Map<String, Object> CONFIG = ImmutableMap.of(
            "ioRegistries",
            ImmutableList.of(HugeGraphIoRegistry.class.getName()));

    private static final BigDecimal DECIMAL = new BigDecimal("1.5");
    private static final BigDecimal WEI = new BigDecimal(
            "0.000000000000000001");
    private static final BigDecimal UINT256_MAX = new BigDecimal(
            "115792089237316195423570985008687907853" +
            "269984665640564039457584007913129639935");

    private static ResponseMessage response(Object... results) {
        return ResponseMessage.build(UUID.randomUUID())
                              .result(ImmutableList.copyOf(results))
                              .create();
    }

    private static Object firstResult(ResponseMessage message) {
        @SuppressWarnings("unchecked")
        List<Object> data = (List<Object>) message.getResult().getData();
        return data.get(0);
    }

    @Test
    public void testBigDecimalThroughGraphSONV1() throws Exception {
        GraphSONMessageSerializerV1d0 serializer =
                new GraphSONMessageSerializerV1d0();
        serializer.configure(CONFIG, null);

        String json = serializer.serializeResponseAsString(response(DECIMAL));
        Assert.assertContains("\"1.5\"", json);
    }

    @Test
    public void testBigDecimalThroughGraphSONV2() throws Exception {
        GraphSONMessageSerializerV2d0 serializer =
                new GraphSONMessageSerializerV2d0();
        serializer.configure(CONFIG, null);
        this.assertTypedRoundTrip(serializer);
    }

    @Test
    public void testBigDecimalThroughGraphSONV3() throws Exception {
        GraphSONMessageSerializerV3d0 serializer =
                new GraphSONMessageSerializerV3d0();
        serializer.configure(CONFIG, null);
        this.assertTypedRoundTrip(serializer);
    }

    private void assertTypedRoundTrip(MessageTextSerializer<?> serializer)
                                      throws SerializationException {
        for (BigDecimal value : ImmutableList.of(DECIMAL, WEI, UINT256_MAX)) {
            String json = serializer.serializeResponseAsString(
                                     response(value));
            // the type prefix survives, the value travels as a plain string
            Assert.assertContains("gx:BigDecimal", json);
            Assert.assertContains("\"" + value.toPlainString() + "\"", json);

            ResponseMessage read = serializer.deserializeResponse(json);
            Object result = firstResult(read);
            Assert.assertEquals(BigDecimal.class, result.getClass());
            Assert.assertEquals(value, result);
        }
    }
}
