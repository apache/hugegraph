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

package org.apache.hugegraph.api.traversers;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        AllShortestPathsApiTest.class,
        CountApiTest.class,
        CrosspointsApiTest.class,
        CustomizedCrosspointsApiTest.class,
        EdgesApiTest.class,
        FusiformSimilarityApiTest.class,
        JaccardSimilarityApiTest.class,
        KneighborApiTest.class,
        KoutApiTest.class,
        MultiNodeShortestPathApiTest.class,
        NeighborRankApiTest.class,
        PathsApiTest.class,
        PersonalRankApiTest.class,
        RaysApiTest.class,
        RingsApiTest.class,
        SameNeighborsApiTest.class,
        SameNeighborsBatchApiTest.class,
        ShortestPathApiTest.class,
        SingleSourceShortestPathApiTest.class,
        TemplatePathsApiTest.class,
        WeightedShortestPathApiTest.class,
        AdamicAdarAPITest.class,
        ResourceAllocationAPITest.class
})
public class TraversersApiTestSuite {

}
