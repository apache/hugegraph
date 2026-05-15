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
# Standalone HBase 2.6.5 image for HugeGraph HBase backend testing.
# Exposes ZooKeeper on 2181 with znode /hbase (matching hugegraph.properties defaults).

FROM eclipse-temurin:11-jre-jammy

ARG HBASE_VERSION=2.6.5
ARG HBASE_PRIMARY_URL=https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz
ARG HBASE_FALLBACK_URL=https://downloads.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz
ARG ALLOW_UNVERIFIED_DOWNLOAD=false

ENV HBASE_VERSION=${HBASE_VERSION}
ENV HBASE_HOME=/opt/hbase
ENV PATH=${HBASE_HOME}/bin:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    download_ok=0; \
    downloaded_url=""; \
    for url in "${HBASE_PRIMARY_URL}" "${HBASE_FALLBACK_URL}"; do \
        [ -n "${url}" ] || continue; \
        echo "Downloading HBase from ${url}"; \
        if curl -fL --retry 8 --retry-delay 5 --retry-all-errors \
                --connect-timeout 30 --max-time 1800 "${url}" -o /tmp/hbase.tar.gz; then \
            download_ok=1; \
            downloaded_url="${url}"; \
            break; \
        fi; \
        echo "Download failed for ${url}, trying next source..."; \
        rm -f /tmp/hbase.tar.gz; \
    done; \
    if [ "${download_ok}" -ne 1 ]; then \
        echo "Unable to download HBase ${HBASE_VERSION} tarball from configured sources"; \
        exit 1; \
    fi; \
    checksum_ok=0; \
    for checksum_url in "${downloaded_url}.sha512" "${HBASE_PRIMARY_URL}.sha512" "${HBASE_FALLBACK_URL}.sha512"; do \
        [ -n "${checksum_url}" ] || continue; \
        if curl -fL --retry 5 --retry-delay 3 --retry-all-errors \
                --connect-timeout 30 "${checksum_url}" -o /tmp/hbase.tar.gz.sha512 2>/dev/null; then \
            checksum_ok=1; \
            break; \
        fi; \
    done; \
    if [ "${checksum_ok}" -eq 1 ]; then \
        echo "Verifying SHA512 checksum..."; \
        expected_hash=$(grep -Eio '[a-f0-9]{128}' /tmp/hbase.tar.gz.sha512 | head -n 1 | tr 'A-F' 'a-f' || true); \
        if [ -z "$expected_hash" ]; then \
            expected_hash=$(awk '{for (i = 1; i <= NF; i++) if (length($i) >= 8 && $i ~ /^[0-9A-Fa-f]+$/) hash = hash $i} END {if (length(hash) >= 128) print tolower(substr(hash, 1, 128))}' /tmp/hbase.tar.gz.sha512 || true); \
        fi; \
        if [ -n "$expected_hash" ]; then \
            actual_hash=$(sha512sum /tmp/hbase.tar.gz | awk '{print $1}'); \
            if [ "$expected_hash" = "$actual_hash" ]; then \
                echo "SHA512 verified OK"; \
            else \
                echo "ERROR: SHA512 mismatch"; \
                echo "  Expected: $expected_hash"; \
                echo "  Actual:   $actual_hash"; \
                exit 1; \
            fi; \
        else \
            if [ "${ALLOW_UNVERIFIED_DOWNLOAD}" = "true" ]; then \
                echo "WARNING: Could not parse SHA512 file (ALLOW_UNVERIFIED_DOWNLOAD=true)"; \
            else \
                echo "ERROR: Could not parse SHA512 file"; \
                exit 1; \
            fi; \
        fi; \
    else \
        if [ "${ALLOW_UNVERIFIED_DOWNLOAD}" = "true" ]; then \
            echo "WARNING: Could not download SHA512 file (ALLOW_UNVERIFIED_DOWNLOAD=true)"; \
        else \
            echo "ERROR: Could not download SHA512 file"; \
            exit 1; \
        fi; \
    fi; \
    tar -xzf /tmp/hbase.tar.gz -C /opt; \
    mv /opt/hbase-${HBASE_VERSION} ${HBASE_HOME}; \
    rm -f /tmp/hbase.tar.gz /tmp/hbase.tar.gz.sha512

# hbase-site.xml: standalone mode, znode=/hbase (matches hugegraph.properties)
COPY hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml

# Entrypoint: start HBase then tail the master log
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 2181 16000 16010 16020 16030

ENTRYPOINT ["/entrypoint.sh"]

