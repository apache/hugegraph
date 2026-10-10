#!/usr/bin/env bash
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

set -euo pipefail
: "${IMAGE_TAG:?set the unique CI image tag}"
for pair in \
    hugegraph-pd/Dockerfile=hugegraph/pd \
    hugegraph-store/Dockerfile=hugegraph/store \
    hugegraph-server/Dockerfile=hugegraph/hugegraph \
    hugegraph-server/Dockerfile-hstore=hugegraph/server; do
    (
        dockerfile="${pair%%=*}"
        IMAGE_ID=$(docker image inspect --format '{{.Id}}' "${pair#*=}:${IMAGE_TAG}")
        echo "Checking ${pair#*=}:${IMAGE_TAG} ($IMAGE_ID)"
        HC=$(docker image inspect --format '{{json .Config.Healthcheck}}' "$IMAGE_ID")
        [[ "$HC" != null ]] || { echo "Missing HEALTHCHECK: $dockerfile"; exit 1; }

        JAVA_VERSION=$(docker run --rm --pull never --entrypoint java "$IMAGE_ID" -version 2>&1)
        echo "$JAVA_VERSION"
        grep -Eq 'version "17\.' <<< "$JAVA_VERSION" || {
            echo "ERROR: expected a Java 17 runtime in $dockerfile"
            exit 1
        }

        if [[ "$dockerfile" == hugegraph-server/* ]]; then
            STATE=$(docker run --rm --pull never "$IMAGE_ID" bash -c \
                'source /hugegraph-server/bin/util.sh && port_listen_state 8080')
            echo "port_listen_state 8080 -> $STATE"
            # Require a usable answer from the socket-table tool in the image.
            [[ "$STATE" == "free" || "$STATE" == "busy" ]] || {
                echo "ERROR: no usable socket-table tool (ss/netstat) in $dockerfile"
                exit 1
            }
        fi

        if [[ "$dockerfile" == hugegraph-server/* ]]; then
            CHECK_DIR=$(mktemp -d)
            trap 'rm -rf "$CHECK_DIR"' EXIT
            docker run --rm --pull never --entrypoint bash \
                -v "$CHECK_DIR:/check" "$IMAGE_ID" -c \
                'cp /hugegraph-server/lib/hugegraph-api-*.jar \
                    /hugegraph-server/lib/hugegraph-common-*.jar /check/'

            API_JAR=$(find "$CHECK_DIR" -name 'hugegraph-api-*.jar' -print -quit)
            COMMON_JAR=$(find "$CHECK_DIR" -name 'hugegraph-common-*.jar' -print -quit)
            EXPECTED_MANIFEST=$(sed -n \
                's|.*<Implementation-Version>\([^<]*\)</Implementation-Version>.*|\1|p' \
                hugegraph-server/hugegraph-api/pom.xml)
            ACTUAL_MANIFEST=$(unzip -p "$API_JAR" META-INF/MANIFEST.MF |
                sed -n 's/^Implementation-Version: *//p' | tr -d '\r')
            EXPECTED_PROPERTY=$(sed -n 's/^ApiVersion=//p' \
                hugegraph-commons/hugegraph-common/src/main/resources/version.properties)
            ACTUAL_PROPERTY=$(unzip -p "$COMMON_JAR" version.properties |
                sed -n 's/^ApiVersion=//p' | tr -d '\r')

            [[ "$ACTUAL_MANIFEST" == "$EXPECTED_MANIFEST" ]] || {
                echo "ERROR: API manifest is $ACTUAL_MANIFEST; expected $EXPECTED_MANIFEST"
                exit 1
            }
            [[ "$ACTUAL_PROPERTY" == "$EXPECTED_PROPERTY" ]] || {
                echo "ERROR: API property is $ACTUAL_PROPERTY; expected $EXPECTED_PROPERTY"
                exit 1
            }
        fi
    )
done
