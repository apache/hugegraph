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
# Select the native runtime without changing the source build or deployment layout.
ARG RUNTIME_VARIANT=standard

# 1st stage: build source code
FROM --platform=$BUILDPLATFORM maven:3.9.16-eclipse-temurin-17 AS build

WORKDIR /pkg

COPY . .

ARG MAVEN_ARGS
ARG MAVEN_PROJECTS="hugegraph-server/hugegraph-dist,hugegraph-pd/hg-pd-dist,hugegraph-store/hg-store-dist"
ARG SOURCE_REVISION=local

RUN --mount=type=cache,id=hugegraph-maven-${SOURCE_REVISION},target=/root/.m2,sharing=locked \
    --mount=type=secret,id=proxy_ca,required=false \
    --mount=type=secret,id=maven_settings,required=false \
    set -e; \
    if [ -f /run/secrets/proxy_ca ]; then \
        cp "$JAVA_HOME/lib/security/cacerts" /tmp/hugegraph-build-cacerts; \
        keytool -importcert -noprompt -alias hugegraph-build-proxy \
            -file /run/secrets/proxy_ca -keystore /tmp/hugegraph-build-cacerts -storepass changeit; \
        export MAVEN_OPTS="${MAVEN_OPTS:-} -Djavax.net.ssl.trustStore=/tmp/hugegraph-build-cacerts"; \
    fi; \
    if [ -f /run/secrets/maven_settings ]; then \
        MAVEN_ARGS="-s /run/secrets/maven_settings ${MAVEN_ARGS:-}"; \
    fi; \
    mvn install -pl "$MAVEN_PROJECTS" \
        -am $MAVEN_ARGS -e -B -ntp -Dmaven.test.skip=true -Dmaven.javadoc.skip=true; \
    rm -f /tmp/hugegraph-build-cacerts; \
    rm ./hugegraph-server/*.tar.gz ./hugegraph-pd/*.tar.gz ./hugegraph-store/*.tar.gz

# Optional local JNI directory: --build-context topling-jni=/path/to/directory.
# An empty default stage keeps ordinary and URL-based builds self-contained.
FROM scratch AS topling-jni

# 2nd stage: runtime env
# Note: ZGC (The Z Garbage Collector) is only supported on ARM-Mac with java > 13
FROM eclipse-temurin:17-jre-jammy AS standard-base
ENV TOPLINGDB_ROCKSDB_PROVIDER=rocksdb

# The fork's JNI requires glibc 2.38+ and a Java 17 JDK for its capability probe.
FROM eclipse-temurin:17-jdk-noble AS topling-base
ARG TARGETARCH
RUN test "$TARGETARCH" = amd64 || { echo 'Topling images require linux/amd64' >&2; exit 1; }
ENV TOPLINGDB_ROCKSDB_PROVIDER=topling

FROM ${RUNTIME_VARIANT}-base AS runtime
ARG RUNTIME_VARIANT
ARG SOURCE_REVISION=local
ARG SOURCE_REPOSITORY=https://github.com/apache/hugegraph
LABEL org.opencontainers.image.source="$SOURCE_REPOSITORY" \
      org.opencontainers.image.revision="$SOURCE_REVISION" \
      org.apache.hugegraph.rocksdb-runtime="$RUNTIME_VARIANT"

LABEL maintainer="HugeGraph Docker Maintainers <dev@hugegraph.apache.org>"

# TODO: use g1gc or zgc as default
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseContainerSupport -XX:MaxRAMPercentage=50 -XshowSettings:vm" \
    HUGEGRAPH_HOME="hugegraph-pd" \
    STDOUT_MODE="true"

WORKDIR /hugegraph-pd/

# 1. Install runtime dependencies
# Note: lsof is gone because its only consumer was the dead check_port removed
# from hg-pd-dist bin/util.sh.  PD runs no port preflight, so unlike the server
# images this needs no socket-table tool in its place.
ARG RUNTIME_DEPS_EPOCH=1
RUN --mount=type=secret,id=proxy_ca,required=false \
    native_deps="" \
    && if [ "$RUNTIME_VARIANT" = topling ]; then \
        native_deps="libaio1t64 libstdc++6 libsnappy1v5 libbz2-1.0 liblz4-1 libzstd1 zlib1g unzip"; \
    fi \
    && apt_ca=/etc/ssl/certs/ca-certificates.crt \
    && if [ -f /run/secrets/proxy_ca ]; then apt_ca=/run/secrets/proxy_ca; fi \
    && apt-get -q -o Acquire::https::CaInfo="$apt_ca" update \
    && apt-get -q -o Acquire::https::CaInfo="$apt_ca" install -y --no-install-recommends --no-install-suggests \
       dumb-init \
       procps \
       curl \
       vim ${native_deps} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Run as a fixed non-root user; USER must be numeric so Kubernetes can
# verify runAsNonRoot.  The data and log directories exist in the image so
# named volumes mounted there start out owned by that user.  See
# docker/README.md before upgrading volumes written by an older image.
ARG HG_UID=10001
ARG HG_GID=10001
RUN groupadd --system --gid "${HG_GID}" hugegraph \
    && useradd --system --uid "${HG_UID}" --gid hugegraph --no-create-home \
       --home-dir /hugegraph-pd --shell /usr/sbin/nologin hugegraph \
    && install -d -o "${HG_UID}" -g "${HG_GID}" /hugegraph-pd /hugegraph-pd/pd_data /hugegraph-pd/logs

COPY --from=build --chown=${HG_UID}:${HG_GID} /pkg/hugegraph-pd/apache-hugegraph-pd-*/ /hugegraph-pd/

# 3. Init docker script
COPY --chown=${HG_UID}:${HG_GID} --chmod=755 hugegraph-pd/hg-pd-dist/docker/docker-entrypoint.sh .

# Download only during an explicit Topling build, verify and prepare with the
# distribution's normal capability probe. No network access is needed at startup.
ARG TOPLING_JNI_URL
ARG TOPLING_JNI_SHA256
COPY docker/prepare-topling-image.sh docker/topling-runtime.env docker/toplingdb.yaml /tmp/
RUN --mount=type=secret,id=proxy_ca,required=false \
    --mount=type=bind,from=topling-jni,target=/run/topling-jni \
    if [ "$RUNTIME_VARIANT" = topling ]; then \
        if [ -f /run/secrets/proxy_ca ]; then export CURL_CA_BUNDLE=/run/secrets/proxy_ca; fi; \
        bash /tmp/prepare-topling-image.sh \
            && chown -R "${HG_UID}:${HG_GID}" topling conf/toplingdb.yaml; \
    fi \
    && rm /tmp/prepare-topling-image.sh /tmp/topling-runtime.env /tmp/toplingdb.yaml

USER ${HG_UID}:${HG_GID}

EXPOSE 8620
# Persist data without hiding a replacement image's binaries and native runtime.
VOLUME /hugegraph-pd/pd_data

HEALTHCHECK --interval=15s --timeout=10s --start-period=90s --retries=3 \
    CMD curl -fsS http://localhost:8620/v1/health >/dev/null

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["./docker-entrypoint.sh"]
