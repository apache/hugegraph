/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.perf;

import java.util.List;

public interface Stopwatch extends Cloneable {

    Path id();

    String name();

    Path parent();

    void startTime(long startTime);

    void endTime(long startTime);

    void lastStartTime(long startTime);

    long times();

    long totalTimes();

    long totalChildrenTimes();

    long totalCost();

    void totalCost(long otherCost);

    long minCost();

    long maxCost();

    long totalWasted();

    long totalSelfWasted();

    long totalChildrenWasted();

    void fillChildrenTotal(List<Stopwatch> children);

    Stopwatch copy();

    Stopwatch child(String name);

    Stopwatch child(String name, Stopwatch watch);

    boolean empty();

    void clear();

    default String toJson() {
        int len = 200 + this.name().length() + this.parent().length();
        StringBuilder sb = new StringBuilder(len);
        sb.append("{");
        sb.append("\"parent\":\"").append(this.parent()).append("\"");
        sb.append(",\"name\":\"").append(this.name()).append("\"");
        sb.append(",\"times\":").append(this.times());
        sb.append(",\"total_cost\":").append(this.totalCost());
        sb.append(",\"min_cost\":").append(this.minCost());
        sb.append(",\"max_cost\":").append(this.maxCost());
        sb.append(",\"total_self_wasted\":").append(this.totalSelfWasted());
        sb.append(",\"total_children_wasted\":").append(
                                                 this.totalChildrenWasted());
        sb.append(",\"total_children_times\":").append(
                                                 this.totalChildrenTimes());
        sb.append("}");
        return sb.toString();
    }

    static Path id(Path parent, String name) {
        if (parent == Path.EMPTY && name == Path.ROOT_NAME) {
            return Path.EMPTY;
        }
        return new Path(parent, name);
    }

    final class Path implements Comparable<Path> {

        public static final String ROOT_NAME = "root";
        public static final Path EMPTY = new Path("");

        private final String path;

        public Path(String self) {
            this.path = self;
        }

        public Path(Path parent, String name) {
            if (parent == EMPTY) {
                this.path = name;
            } else {
                this.path = parent.path + '/' + name;
            }
        }

        public int length() {
            return this.path.length();
        }

        @Override
        public int hashCode() {
            return this.path.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Path)) {
                return false;
            }
            if (this.hashCode() != obj.hashCode()) {
                return false;
            }
            Path other = (Path) obj;
            return this.path.equals(other.path);
        }

        @Override
        public int compareTo(Path other) {
            return this.path.compareTo(other.path);
        }

        @Override
        public String toString() {
            return this.path;
        }

        public boolean endsWith(String name) {
            return this.path.endsWith(name);
        }
    }
}
