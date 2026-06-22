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

# Dockerfile for HugeGraph Server
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

COPY --from=build /pkg/hugegraph-server/apache-hugegraph-server-incubating-*/ /hugegraph-server/
LABEL maintainer="HugeGraph Docker Maintainers <dev@hugegraph.apache.org>"

# TODO: use g1gc or zgc as default
# Note: --add-exports is required for Java 11+ to access jdk.internal.reflect for auth proxy
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseContainerSupport -XX:MaxRAMPercentage=50 -XshowSettings:vm --add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED" \
    HUGEGRAPH_HOME="hugegraph-server"

#COPY . /hugegraph/hugegraph-server
WORKDIR /hugegraph-server/

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
    && pwd && cd /hugegraph-server/ \
    && sed -i "s/^restserver.url.*$/restserver.url=http:\/\/0.0.0.0:8080/g" ./conf/rest-server.properties

# 2. Init docker script
COPY hugegraph-server/hugegraph-dist/docker/scripts/remote-connect.groovy ./scripts
COPY hugegraph-server/hugegraph-dist/docker/scripts/detect-storage.groovy ./scripts
COPY hugegraph-server/hugegraph-dist/docker/docker-entrypoint.sh .
RUN chmod 755 ./docker-entrypoint.sh

EXPOSE 8080
VOLUME /hugegraph-server

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["./docker-entrypoint.sh"]
