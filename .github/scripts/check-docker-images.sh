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

        case "$dockerfile" in
            hugegraph-pd/Dockerfile)
                EXPECTED_VOLUME=/hugegraph-pd/pd_data
                ;;
            hugegraph-store/Dockerfile)
                EXPECTED_VOLUME=/hugegraph-store/storage
                ;;
            hugegraph-server/Dockerfile)
                EXPECTED_VOLUME=/hugegraph-server/rocksdb-data
                ;;
            hugegraph-server/Dockerfile-hstore)
                EXPECTED_VOLUME=
                ;;
        esac
        ACTUAL_VOLUMES=$(docker image inspect --format \
            '{{range $path, $_ := .Config.Volumes}}{{$path}}{{"\n"}}{{end}}' \
            "$IMAGE_ID" | sed '/^$/d' | sort)
        [[ "$ACTUAL_VOLUMES" == "$EXPECTED_VOLUME" ]] || {
            echo "ERROR: $dockerfile declares volumes [$ACTUAL_VOLUMES]; expected [$EXPECTED_VOLUME]"
            exit 1
        }
        if [[ "$dockerfile" != hugegraph-server/Dockerfile-hstore ]]; then
            expected_provider=rocksdb
            if [[ "${RUNTIME_VARIANT:-standard}" == topling ]]; then expected_provider=topling; fi
            docker image inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$IMAGE_ID" |
                grep -qx "TOPLINGDB_ROCKSDB_PROVIDER=$expected_provider"
        fi
        # The images must run as the fixed non-root user and own the directories
        # the start scripts and default volumes write to.
        case "$dockerfile" in
            hugegraph-pd/*) OWNED_DIRS=". logs pd_data" ;;
            hugegraph-store/*) OWNED_DIRS=". logs storage" ;;
            hugegraph-server/Dockerfile) OWNED_DIRS=". logs rocksdb-data" ;;
            *) OWNED_DIRS=". logs" ;;
        esac
        IMAGE_USER=$(docker image inspect --format '{{.Config.User}}' "$IMAGE_ID")
        RUNTIME_IDS=$(docker run --rm --pull never --entrypoint sh "$IMAGE_ID" -c \
            'echo "$(id -u):$(id -g)" $(stat -c %u:%g "$@")' owners $OWNED_DIRS)
        EXPECTED_IDS="10001:10001$(printf ' 10001:10001%.0s' $OWNED_DIRS)"
        [[ "$IMAGE_USER" == 10001:10001 && "$RUNTIME_IDS" == "$EXPECTED_IDS" ]] || {
            echo "ERROR: $dockerfile runs as '$IMAGE_USER' ($RUNTIME_IDS); expected 10001:10001 owning $OWNED_DIRS"
            exit 1
        }

        # Helm reuses these images for tool containers instead of pulling curlimages/curl.
        # Check the same non-root/read-only tool environment as the Server test hook.
        if [[ "$dockerfile" == hugegraph-store/* || "$dockerfile" == hugegraph-server/* ]]; then
            HELM_TOOLS=(curl tr date sleep)
            if [[ "$dockerfile" == hugegraph-server/* ]]; then
                HELM_TOOLS+=(getent base64 awk grep sort tail)
            fi
            docker run --rm --pull never --read-only --user 100:101 --entrypoint sh "$IMAGE_ID" -c '
                for tool in "$@"; do
                    command -v "$tool" >/dev/null || {
                        echo "Missing Helm tool: $tool" >&2
                        exit 1
                    }
                    case "$tool" in
                        getent) getent ahosts localhost >/dev/null ;;
                        base64) test "$(printf Helm | base64 | tr -d "\n")" = SGVsbQ== ;;
                    esac
                done
                curl --version >/dev/null
            ' helm-tools "${HELM_TOOLS[@]}"
        fi

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
            # The image user cannot write the runner-owned temp dir, so copy
            # the jars out as the runner's own uid.
            docker run --rm --pull never --user "$(id -u):$(id -g)" --entrypoint bash \
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
