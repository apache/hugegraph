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

package org.example.hugegraph.cloud.sample;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.rocksdb.access.cloud.CloudStorageClient;
import org.apache.hugegraph.rocksdb.access.cloud.CloudStorageProvider;

public class SampleCloudStorageProvider implements CloudStorageProvider {

    @Override
    public String name() {
        return "sample";
    }

    @Override
    public CloudStorageClient create(HugeConfig config) {
        // Keep the template minimal: real plugins should parse provider-specific
        // keys from HugeConfig and initialize their cloud SDK clients.
        return new SampleCloudStorageClient("", "", "");
    }
}

