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

package org.apache.hugegraph.unit.core;

import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.junit.Test;

/**
 * Only a DECIMAL key normalizes its ~default_value eagerly; every other type
 * keeps the raw value the user sent (and an unconvertible stored default
 * still loads), exactly as on master.
 */
public class PropertyKeyUserdataTest {

    private static PropertyKey key(DataType type, Cardinality cardinality) {
        PropertyKey pkey = new PropertyKey(null, IdGenerator.of(1), "k");
        pkey.dataType(type);
        pkey.cardinality(cardinality);
        return pkey;
    }

    @Test
    public void testDecimalDefaultIsExact() {
        PropertyKey pkey = key(DataType.DECIMAL, Cardinality.SINGLE);
        pkey.userdata(Userdata.DEFAULT_VALUE, new BigDecimal("0.1234567890123456789"));
        Assert.assertEquals(new BigDecimal("0.1234567890123456789"),
                            pkey.userdata().get(Userdata.DEFAULT_VALUE));
        pkey.userdata(Userdata.DEFAULT_VALUE, "1.50");
        Assert.assertEquals(new BigDecimal("1.50"), pkey.userdata().get(Userdata.DEFAULT_VALUE));
        PropertyKey list = key(DataType.DECIMAL, Cardinality.LIST);
        list.userdata(Userdata.DEFAULT_VALUE, Arrays.asList(new BigDecimal("1.5"), 2));
        Assert.assertEquals(Arrays.asList(new BigDecimal("1.5"), new BigDecimal("2")),
                            list.userdata().get(Userdata.DEFAULT_VALUE));
        // a stored default that does not convert keeps loading (per-entry path)...
        pkey.userdata(Userdata.DEFAULT_VALUE, "not-a-number");
        Assert.assertEquals("not-a-number", pkey.userdata().get(Userdata.DEFAULT_VALUE));
        // ...but a create/append through the builder (bulk path) rejects it
        Userdata bad = new Userdata();
        bad.put(Userdata.DEFAULT_VALUE, "not-a-number");
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            key(DataType.DECIMAL, Cardinality.SINGLE).userdata(bad);
        });
        Userdata huge = new Userdata();
        huge.put(Userdata.DEFAULT_VALUE, new BigDecimal("1E+999999999"));
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            key(DataType.DECIMAL, Cardinality.SINGLE).userdata(huge);
        });
        Userdata good = new Userdata();
        good.put(Userdata.DEFAULT_VALUE, "0.5");
        PropertyKey ok = key(DataType.DECIMAL, Cardinality.SINGLE);
        ok.userdata(good);
        Assert.assertEquals(new BigDecimal("0.5"), ok.userdata().get(Userdata.DEFAULT_VALUE));
    }

    @Test
    public void testOtherTypesKeepTheRawDefault() {
        PropertyKey date = key(DataType.DATE, Cardinality.SINGLE);
        date.userdata(Userdata.DEFAULT_VALUE, "2020-01-01");
        Assert.assertEquals("2020-01-01", date.userdata().get(Userdata.DEFAULT_VALUE));
        date.userdata(Userdata.DEFAULT_VALUE, "not-a-date");
        Assert.assertEquals("not-a-date", date.userdata().get(Userdata.DEFAULT_VALUE));

        PropertyKey ints = key(DataType.INT, Cardinality.LIST);
        ints.userdata(Userdata.DEFAULT_VALUE, 1);
        Assert.assertEquals(1, ints.userdata().get(Userdata.DEFAULT_VALUE));

        PropertyKey dbl = key(DataType.DOUBLE, Cardinality.SINGLE);
        Userdata userdata = new Userdata();
        userdata.put(Userdata.DEFAULT_VALUE, 1.5d);
        userdata.put("rate", 0.85d);
        dbl.userdata(userdata);
        Assert.assertEquals(1.5d, dbl.userdata().get(Userdata.DEFAULT_VALUE));
        Assert.assertEquals(0.85d, dbl.userdata().get("rate"));
        // the API reads the default exactly; for a non-decimal key that
        // BigDecimal becomes the Double the parser gave on master
        dbl.userdata(Userdata.DEFAULT_VALUE, new BigDecimal("1.5"));
        Assert.assertEquals(1.5d, dbl.userdata().get(Userdata.DEFAULT_VALUE));
        PropertyKey dbls = key(DataType.DOUBLE, Cardinality.LIST);
        dbls.userdata(Userdata.DEFAULT_VALUE, Arrays.asList(new BigDecimal("1.5"), 2));
        Assert.assertEquals(Arrays.asList(1.5d, 2), dbls.userdata().get(Userdata.DEFAULT_VALUE));
        ints.userdata(Userdata.DEFAULT_VALUE, new BigDecimal("1.5"));
        Assert.assertEquals(1.5d, ints.userdata().get(Userdata.DEFAULT_VALUE));
    }
}
