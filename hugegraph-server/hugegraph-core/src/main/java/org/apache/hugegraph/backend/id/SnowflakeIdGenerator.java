/*
 * Copyright 2010-2012 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.id;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.TimeUtil;
import org.slf4j.Logger;

public class SnowflakeIdGenerator extends IdGenerator {

    private static final Logger LOG = Log.logger(SnowflakeIdGenerator.class);

    private static final Map<String, SnowflakeIdGenerator> INSTANCES =
            new ConcurrentHashMap<>();

    private final boolean forceString;
    private final IdWorker idWorker;

    public static SnowflakeIdGenerator init(HugeGraphParams graph) {
        String graphName = graph.graph().spaceGraphName();
        SnowflakeIdGenerator generator = INSTANCES.get(graphName);
        if (generator == null) {
            synchronized (INSTANCES) {
                if (!INSTANCES.containsKey(graphName)) {
                    HugeConfig conf = graph.configuration();
                    INSTANCES.put(graphName, new SnowflakeIdGenerator(conf));
                }
                generator = INSTANCES.get(graphName);
                assert generator != null;
            }
        }
        return generator;
    }

    public static SnowflakeIdGenerator instance(HugeGraph graph) {
        String graphName = graph.spaceGraphName();
        SnowflakeIdGenerator generator = INSTANCES.get(graphName);
        E.checkState(generator != null,
                     "SnowflakeIdGenerator of graph '%s' is not initialized",
                     graphName);
        return generator;
    }

    private SnowflakeIdGenerator(HugeConfig config) {
        long workerId = config.get(CoreOptions.SNOWFLAKE_WORKER_ID);
        long datacenterId = config.get(CoreOptions.SNOWFLAKE_DATACENTER_ID);
        this.forceString = config.get(CoreOptions.SNOWFLAKE_FORCE_STRING);
        this.idWorker = new IdWorker(workerId, datacenterId);
        LOG.debug("SnowflakeId Worker started: datacenter id {}, " +
                  "worker id {}, forced string id {}",
                  datacenterId, workerId, this.forceString);
    }

    public Id generate() {
        if (this.idWorker == null) {
            throw new HugeException("Please initialize before using");
        }
        Id id = of(this.idWorker.nextId());
        if (!this.forceString) {
            return id;
        } else {
            return IdGenerator.of(id.asString());
        }
    }

    @Override
    public Id generate(HugeVertex vertex) {
        return this.generate();
    }

    private static class IdWorker {

        private final long workerId;
        private final long datacenterId;
        private long sequence = 0L; // AtomicLong
        private long lastTimestamp = -1L;

        private static final long WORKER_BIT = 5L;
        private static final long MAX_WORKER_ID = -1L ^ (-1L << WORKER_BIT);

        private static final long DC_BIT = 5L;
        private static final long MAX_DC_ID = -1L ^ (-1L << DC_BIT);

        private static final long SEQUENCE_BIT = 12L;
        private static final long SEQUENCE_MASK = -1L ^ (-1L << SEQUENCE_BIT);

        private static final long WORKER_SHIFT = SEQUENCE_BIT;
        private static final long DC_SHIFT = WORKER_SHIFT + WORKER_BIT;
        private static final long TIMESTAMP_SHIFT = DC_SHIFT + DC_BIT;

        public IdWorker(long workerId, long datacenterId) {
            // Sanity check for workerId
            if (workerId > MAX_WORKER_ID || workerId < 0) {
                throw new IllegalArgumentException(String.format(
                        "Worker id can't > %d or < 0",
                        MAX_WORKER_ID));
            }
            if (datacenterId > MAX_DC_ID || datacenterId < 0) {
                throw new IllegalArgumentException(String.format(
                        "Datacenter id can't > %d or < 0",
                        MAX_DC_ID));
            }
            this.workerId = workerId;
            this.datacenterId = datacenterId;
            LOG.debug("Id Worker starting. timestamp left shift {}," +
                      "datacenter id bits {}, worker id bits {}," +
                      "sequence bits {}",
                      TIMESTAMP_SHIFT, DC_BIT, WORKER_BIT, SEQUENCE_BIT);
        }

        public synchronized long nextId() {
            long timestamp = TimeUtil.timeGen();

            if (timestamp > this.lastTimestamp) {
                this.sequence = 0L;
            } else if (timestamp == this.lastTimestamp) {
                this.sequence = (this.sequence + 1) & SEQUENCE_MASK;
                if (this.sequence == 0) {
                    timestamp = TimeUtil.tillNextMillis(this.lastTimestamp);
                }
            } else {
                LOG.error("Clock is moving backwards, " +
                          "rejecting requests until {}.",
                          this.lastTimestamp);
                throw new HugeException("Clock moved backwards. Refusing to " +
                                        "generate id for %d milliseconds",
                                        this.lastTimestamp - timestamp);
            }

            this.lastTimestamp = timestamp;

            return (timestamp << TIMESTAMP_SHIFT) |
                   (this.datacenterId << DC_SHIFT) |
                   (this.workerId << WORKER_SHIFT) |
                   (this.sequence);
        }
    }
}
