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
FROM maven:3.9.0-eclipse-temurin-11 AS build

COPY . /pkg
WORKDIR /pkg
ARG MAVEN_ARGS

RUN mvn package $MAVEN_ARGS -e -B -ntp -Dmaven.test.skip=true -Dmaven.javadoc.skip=true && pwd && ls -l && rm \
    ./hugegraph-server/*.tar.gz && rm ./hugegraph-pd/*.tar.gz && rm ./hugegraph-store/*.tar.gz

# 2nd stage: runtime env
# Note: ZGC (The Z Garbage Collector) is only supported on ARM-Mac with java > 13
FROM eclipse-temurin:11-jre

COPY --from=build /pkg/hugegraph-store/apache-hugegraph-store-incubating-*/ /hugegraph-store/
LABEL maintainer="HugeGraph Docker Maintainers <dev@hugegraph.apache.org>"

# TODO: use g1gc or zgc as default
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseContainerSupport -XX:MaxRAMPercentage=50 -XshowSettings:vm" \
    HUGEGRAPH_HOME="hugegraph-store"

#COPY . /hugegraph/hugegraph-store
WORKDIR /hugegraph-store/

# 1. Install environment and init HugeGraph Sever
RUN set -x \
    && rm /var/lib/dpkg/info/libc-bin.* \
    && apt-get -q clean \
    && apt-get -q update \
    && apt-get -q install -y --no-install-recommends --no-install-suggests \
       dumb-init \
       procps \
       curl \
       lsof \
       vim \
       cron \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && service cron start \
    && pwd && cd /hugegraph-store/

# 2. Init docker script
COPY hugegraph-store/hg-store-dist/docker/docker-entrypoint.sh .
RUN chmod 755 ./docker-entrypoint.sh

EXPOSE 8520
VOLUME /hugegraph-store

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["./docker-entrypoint.sh"]
