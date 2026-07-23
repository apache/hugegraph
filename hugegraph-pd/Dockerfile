# syntax=docker/dockerfile:1
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Dockerfile for HugeGraph PD
# 1st stage: build source code
FROM --platform=$BUILDPLATFORM maven:3.9.0-eclipse-temurin-11 AS build

WORKDIR /pkg

COPY . .

ARG MAVEN_ARGS

RUN --mount=type=cache,target=/root/.m2 \
    mvn package $MAVEN_ARGS -e -B -ntp -Dmaven.test.skip=true -Dmaven.javadoc.skip=true \
    && rm ./hugegraph-server/*.tar.gz ./hugegraph-pd/*.tar.gz ./hugegraph-store/*.tar.gz

# 2nd stage: runtime env
# Note: ZGC (The Z Garbage Collector) is only supported on ARM-Mac with java > 13
FROM eclipse-temurin:11-jre-jammy

COPY --from=build /pkg/hugegraph-pd/apache-hugegraph-pd-*/ /hugegraph-pd/
LABEL maintainer="HugeGraph Docker Maintainers <dev@hugegraph.apache.org>"

# TODO: use g1gc or zgc as default
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseContainerSupport -XX:MaxRAMPercentage=50 -XshowSettings:vm" \
    HUGEGRAPH_HOME="hugegraph-pd" \
    STDOUT_MODE="true"

WORKDIR /hugegraph-pd/

# 1. Install runtime dependencies
RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends --no-install-suggests \
       dumb-init \
       procps \
       curl \
       lsof \
       vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Init docker script
COPY hugegraph-pd/hg-pd-dist/docker/docker-entrypoint.sh .
RUN chmod 755 ./docker-entrypoint.sh

EXPOSE 8620
VOLUME /hugegraph-pd

HEALTHCHECK --interval=15s --timeout=10s --start-period=90s --retries=3 \
    CMD curl -fsS http://localhost:8620/v1/health >/dev/null

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["./docker-entrypoint.sh"]
