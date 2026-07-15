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

package org.apache.hugegraph.store.cloud;

/** Test-only SPI provider used to exercise CloudStorageProviderFactory.loadProviders(). */
public class TestDuplicateNamedProvider implements CloudStorageProvider {

    public static final String PROVIDER_NAME = "test-duplicate-provider";

    @Override
    public String providerName() {
        return PROVIDER_NAME;
    }

    @Override
    public void init(CloudStorageConfig config) {
        // no-op for test provider
    }

    @Override
    public void uploadFile(String localPath, String remoteKey) {
        // no-op for test provider
    }

    @Override
    public void deleteFile(String remoteKey) {
        // no-op for test provider
    }

    @Override
    public boolean fileExists(String remoteKey) {
        return false;
    }

    @Override
    public void downloadFile(String remoteKey, String localPath) {
        // no-op for test provider
    }

    @Override
    public void close() {
        // no-op for test provider
    }
}
