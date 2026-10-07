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

package org.apache.hugegraph.serializer;

import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class OlapKeyTest {

    @Test
    public void testMergedKeyMatchesObservedRuntimeRequest() {
        Assert.assertArrayEquals(Bytes.fromHex("080586313a616c696365"),
                OlapKey.format(IdGenerator.of(5), IdGenerator.of("1:alice")));
        BytesBuffer key = BytesBuffer.wrap(OlapKey.format(IdGenerator.of(130), IdGenerator.of(1000)));
        Assert.assertEquals(IdGenerator.of(130), key.readId());
        Assert.assertEquals(IdGenerator.of(1000), key.readId());
        Assert.assertEquals(0, key.remaining());
    }

    @Test
    public void testValueIdentityUsesWholeVariableLengthPropertyId() {
        byte[] observed = Bytes.fromHex("0563");
        Assert.assertTrue(OlapKey.matchesProperty(observed, IdGenerator.of(5)));
        Assert.assertFalse(OlapKey.matchesProperty(observed, IdGenerator.of(8)));
        byte[] largeProperty = BytesBuffer.allocate(16).writeVInt(130).writeVInt(99).bytes();
        Assert.assertTrue(OlapKey.matchesProperty(largeProperty, IdGenerator.of(130)));
        Assert.assertFalse(OlapKey.matchesProperty(largeProperty, IdGenerator.of(1)));
        Assert.assertFalse(OlapKey.matchesProperty(null, IdGenerator.of(5)));
        Assert.assertFalse(OlapKey.matchesProperty(new byte[0], IdGenerator.of(5)));
        Assert.assertFalse(OlapKey.matchesProperty(new byte[]{(byte) 0x81}, IdGenerator.of(5)));
        Assert.assertFalse(OlapKey.matchesProperty(new byte[]{(byte) 0x80}, IdGenerator.of(5)));
    }
}
