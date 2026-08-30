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

package org.apache.hugegraph.backend.store;

/**
 * Shard is used for distributed backend storage scanning.
 * operations. Each shard represents a range of tokens for a node.
 * Reading data from a given shard does not cross multiple nodes.
 */
public class Shard {

    // token range start
    private String start;
    // token range end
    private String end;
    // partitions count in this range
    private long length;

    public Shard(String start, String end, long length) {
        this.start = start;
        this.end = end;
        this.length = length;
    }

    public String start() {
        return this.start;
    }

    public void start(String start) {
        this.start = start;
    }

    public String end() {
        return this.end;
    }

    public void end(String end) {
        this.end = end;
    }

    public long length() {
        return this.length;
    }

    public void length(long length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return String.format("Shard{start=%s, end=%s, length=%s}",
                             this.start, this.end, this.length);
    }
}
