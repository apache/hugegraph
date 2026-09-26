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

import java.util.Arrays;

import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class HugeFactoryTest {

    @Test
    public void testCleanupContinuesAfterGraphFailure() {
        StandardHugeGraph failed = Mockito.mock(StandardHugeGraph.class);
        StandardHugeGraph cleaned = Mockito.mock(StandardHugeGraph.class);
        Mockito.doThrow(new HugeException("test"))
               .when(failed).closeCurrentThreadTransaction();

        Assert.assertThrows(HugeException.class, () -> {
            HugeFactory.closeCurrentThreadTransactions(
                    Arrays.asList(failed, cleaned));
        });

        Mockito.verify(failed).closeCurrentThreadTransaction();
        Mockito.verify(cleaned).closeCurrentThreadTransaction();
    }
}
