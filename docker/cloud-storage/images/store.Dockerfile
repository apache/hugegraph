#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# syntax=docker/dockerfile:1.4
ARG JAVA_RUNTIME_IMAGE=eclipse-temurin:11-jre
FROM ${JAVA_RUNTIME_IMAGE}

WORKDIR /hugegraph-store
COPY docker/cloud-storage/.artifacts/store-dist/ /hugegraph-store/
COPY docker/cloud-storage/.artifacts/plugins/ /hugegraph-store/plugins/
COPY docker/cloud-storage/entrypoints/store-entrypoint.sh /hugegraph-store/store-entrypoint.sh

RUN chmod +x /hugegraph-store/store-entrypoint.sh /hugegraph-store/bin/*.sh && \
    mkdir -p /hugegraph-store/logs /hugegraph-store/storage /hugegraph-store/plugins

EXPOSE 8500 8510 8520
ENTRYPOINT ["/hugegraph-store/store-entrypoint.sh"]
