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

package org.apache.hugegraph.backend.store.cassandra;

import static org.apache.hugegraph.config.OptionChecker.allowValues;
import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.positiveInt;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import org.apache.hugegraph.config.ConfigListOption;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

public class CassandraOptions extends OptionHolder {

    private CassandraOptions() {
        super();
    }

    private static volatile CassandraOptions instance;

    public static synchronized CassandraOptions instance() {
        if (instance == null) {
            instance = new CassandraOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> CASSANDRA_HOST =
            new ConfigOption<>(
                    "cassandra.host",
                    "The seeds hostname or ip address of cassandra cluster.",
                    disallowEmpty(),
                    "localhost"
            );

    public static final ConfigOption<Integer> CASSANDRA_PORT =
            new ConfigOption<>(
                    "cassandra.port",
                    "The seeds port address of cassandra cluster.",
                    rangeInt(1, 65535),
                    9042
            );

    public static final ConfigOption<String> CASSANDRA_USERNAME =
            new ConfigOption<>(
                    "cassandra.username",
                    "The username to use to login to cassandra cluster.",
                    ""
            );

    public static final ConfigOption<String> CASSANDRA_PASSWORD =
            new ConfigOption<>(
                    "cassandra.password",
                    "The password corresponding to cassandra.username.",
                    ""
            );

    public static final ConfigOption<Integer> CASSANDRA_CONN_TIMEOUT =
            new ConfigOption<>(
                    "cassandra.connect_timeout",
                    "The cassandra driver connect server timeout(seconds).",
                    rangeInt(1, 30),
                    5
            );

    public static final ConfigOption<Integer> CASSANDRA_READ_TIMEOUT =
            new ConfigOption<>(
                    "cassandra.read_timeout",
                    "The cassandra driver read from server timeout(seconds).",
                    rangeInt(1, 120),
                    20
            );

    public static final ConfigOption<String> CASSANDRA_STRATEGY =
            new ConfigOption<>(
                    "cassandra.keyspace.strategy",
                    "The replication strategy of keyspace, valid value is " +
                    "SimpleStrategy or NetworkTopologyStrategy.",
                    allowValues("SimpleStrategy", "NetworkTopologyStrategy"),
                    "SimpleStrategy"
            );

    public static final ConfigListOption<String> CASSANDRA_REPLICATION =
            new ConfigListOption<>(
                    "cassandra.keyspace.replication",
                    "The keyspace replication factor of SimpleStrategy, " +
                    "like '[3]'. Or replicas in each datacenter of " +
                    "NetworkTopologyStrategy, like '[dc1:2,dc2:1]'.",
                    disallowEmpty(),
                    "3"
            );

    public static final ConfigOption<String> CASSANDRA_COMPRESSION =
            new ConfigOption<>(
                    "cassandra.compression_type",
                    "The compression algorithm of cassandra transport: none/snappy/lz4.",
                    allowValues("none", "snappy", "lz4"),
                    "none"
            );

    public static final ConfigOption<Integer> CASSANDRA_JMX_PORT =
            new ConfigOption<>(
                    "cassandra.jmx_port",
                    "The port of JMX API service for cassandra",
                    rangeInt(1, 65535),
                    7199
            );

    public static final ConfigOption<Integer> AGGR_TIMEOUT =
            new ConfigOption<>(
                    "cassandra.aggregation_timeout",
                    "The timeout in seconds of waiting for aggregation.",
                    positiveInt(),
                    12 * 60 * 60
            );

    public static final ConfigOption<Long> CASSANDRA_RECONNECT_BASE_DELAY =
            new ConfigOption<>(
                    "cassandra.reconnect_base_delay",
                    "The base delay in milliseconds used by the driver's " +
                    "exponential reconnection policy when a Cassandra host " +
                    "becomes unreachable.",
                    rangeInt(100L, Long.MAX_VALUE),
                    1000L
            );

    public static final ConfigOption<Long> CASSANDRA_RECONNECT_MAX_DELAY =
            new ConfigOption<>(
                    "cassandra.reconnect_max_delay",
                    "The maximum delay in milliseconds used by the driver's " +
                    "exponential reconnection policy when a Cassandra host " +
                    "becomes unreachable.",
                    rangeInt(1000L, Long.MAX_VALUE),
                    10_000L
            );

    public static final ConfigOption<Integer> CASSANDRA_QUERY_RETRY_MAX_ATTEMPTS =
            new ConfigOption<>(
                    "cassandra.query_retry_max_attempts",
                    "The maximum number of retry attempts applied at query-time when " +
                    "a Cassandra host is temporarily unreachable. " +
                    "OperationTimedOutException is retried only for " +
                    "idempotent statements. " +
                    "Set to 0 to disable query-time retries.",
                    rangeInt(0, Integer.MAX_VALUE),
                    3
            );

    public static final ConfigOption<Long> CASSANDRA_QUERY_RETRY_INTERVAL =
            new ConfigOption<>(
                    "cassandra.query_retry_interval",
                    "The interval in milliseconds between query-time retries " +
                    "when a Cassandra host is temporarily unreachable. The " +
                    "actual wait grows with exponential backoff, capped at " +
                    "cassandra.reconnect_max_delay.",
                    rangeInt(100L, Long.MAX_VALUE),
                    1000L
            );
}
