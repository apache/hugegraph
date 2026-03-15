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

package org.apache.hugegraph.pd.core;

import org.apache.hugegraph.pd.core.meta.MetadataKeyHelperTest;
import org.apache.hugegraph.pd.core.store.HgKVStoreImplTest;
import org.apache.hugegraph.pd.raft.IpAuthHandlerTest;
import org.apache.hugegraph.pd.raft.RaftEngineIpAuthIntegrationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import lombok.extern.slf4j.Slf4j;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        MetadataKeyHelperTest.class,
        HgKVStoreImplTest.class,
        ConfigServiceTest.class,
        IdServiceTest.class,
        KvServiceTest.class,
        LogServiceTest.class,
        PartitionServiceTest.class,
        StoreMonitorDataServiceTest.class,
        StoreServiceTest.class,
        TaskScheduleServiceTest.class,
        IpAuthHandlerTest.class,
        RaftEngineIpAuthIntegrationTest.class,
        // StoreNodeServiceTest.class,
})
@Slf4j
public class PDCoreSuiteTest {

}
