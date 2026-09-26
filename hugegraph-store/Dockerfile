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

# Dockerfile for HugeGraph Store
# 1st stage: build source code
FROM --platform=$BUILDPLATFORM maven:3.9.0-eclipse-temurin-11 AS build

WORKDIR /pkg

COPY . .

ARG MAVEN_ARGS
ARG MAVEN_PROJECTS="hugegraph-server/hugegraph-dist,hugegraph-pd/hg-pd-dist,hugegraph-store/hg-store-dist"
ARG SOURCE_REVISION=local

RUN --mount=type=cache,id=hugegraph-maven-${SOURCE_REVISION},target=/root/.m2,sharing=locked \
    mvn install -pl "$MAVEN_PROJECTS" \
        -am $MAVEN_ARGS -e -B -ntp -Dmaven.test.skip=true -Dmaven.javadoc.skip=true \
    && rm ./hugegraph-server/*.tar.gz ./hugegraph-pd/*.tar.gz ./hugegraph-store/*.tar.gz

# Shared runtime environment
# Note: ZGC (The Z Garbage Collector) is only supported on ARM-Mac with java > 13
FROM eclipse-temurin:11-jre-noble AS runtime

LABEL maintainer="HugeGraph Docker Maintainers <dev@hugegraph.apache.org>"

# TODO: use g1gc or zgc as default
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseContainerSupport -XX:MaxRAMPercentage=50 -XshowSettings:vm" \
    HUGEGRAPH_HOME="hugegraph-store" \
    STDOUT_MODE="true"

WORKDIR /hugegraph-store/

# 1. Install runtime dependencies
# Note: lsof is gone because its only consumer was the dead check_port removed
# from hg-store-dist bin/util.sh.  Store runs no port preflight, so unlike the
# server images this needs no socket-table tool in its place.
ARG RUNTIME_DEPS_EPOCH=1
RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends --no-install-suggests \
       dumb-init \
       procps \
       curl \
       util-linux \
       vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Init docker script
COPY hugegraph-store/hg-store-dist/docker/docker-entrypoint.sh .
RUN chmod 755 ./docker-entrypoint.sh

EXPOSE 8520

HEALTHCHECK --interval=15s --timeout=10s --start-period=90s --retries=3 \
    CMD curl -fsS http://localhost:8520/v1/health >/dev/null

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["./docker-entrypoint.sh"]

# Topling runtime packages. The current native library requires glibc 2.38+.
FROM runtime AS topling-runtime

RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends --no-install-suggests \
       libaio1t64 \
       libbz2-1.0 \
       libjemalloc2 \
       liblz4-1 \
       libsnappy1v5 \
       liburing2 \
       zlib1g \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Build the formal component-local Topling distribution on Linux amd64.
FROM topling-runtime AS build-topling

COPY --from=build /pkg/ /pkg/
WORKDIR /pkg
RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends --no-install-suggests \
       rsync \
       unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && STORE_DIR=$(find /pkg/hugegraph-store -maxdepth 1 -type d \
                     -name 'apache-hugegraph-store-*' ! -name '*-topling' \
                     -print -quit) \
    && test -n "$STORE_DIR" \
    && VERSION=${STORE_DIR##*/apache-hugegraph-store-} \
    && install-dist/scripts/build-topling-distribution.sh store "$VERSION"

# Topling image: contains both providers and selects ToplingDB by default.
# Build with: docker build --platform linux/amd64 --target topling ...
FROM topling-runtime AS topling

ARG SOURCE_REPOSITORY=https://github.com/apache/hugegraph
ARG SOURCE_REVISION=local
LABEL org.opencontainers.image.source="${SOURCE_REPOSITORY}" \
      org.opencontainers.image.revision="${SOURCE_REVISION}"

LABEL org.apache.hugegraph.rocksdb-runtime="topling"
COPY --from=build-topling /pkg/hugegraph-store/apache-hugegraph-store-*-topling/ /hugegraph-store/
ENV HG_STORE_ROCKSDB_PROVIDER="topling" \
    HG_STORE_ENFORCE_PROVIDER_MARKER="true"
RUN mkdir -p /hugegraph-store/storage /hugegraph-store/topling-storage
VOLUME ["/hugegraph-store/storage", "/hugegraph-store/topling-storage"]

# Keep the standard image as the final/default target for existing builds.
FROM runtime AS standard

ARG SOURCE_REPOSITORY=https://github.com/apache/hugegraph
ARG SOURCE_REVISION=local
LABEL org.opencontainers.image.source="${SOURCE_REPOSITORY}" \
      org.opencontainers.image.revision="${SOURCE_REVISION}"

LABEL org.apache.hugegraph.rocksdb-runtime="standard"
COPY --from=build /pkg/hugegraph-store/apache-hugegraph-store-*/ /hugegraph-store/
ENV HG_STORE_ROCKSDB_PROVIDER="rocksdb"
# Preserve the historical root volume for anonymous-volume upgrades.
VOLUME /hugegraph-store
