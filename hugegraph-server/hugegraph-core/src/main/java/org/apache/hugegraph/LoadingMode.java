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

package org.apache.hugegraph;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

/** Same graph-instance ownership; this state is neither persisted nor distributed. */
public final class LoadingMode {

    private static final Logger LOG = Log.logger(LoadingMode.class);

    private static final int MAX_ACTIVE = 1024;
    private static final int MAX_RELEASED = 1024;

    private final Supplier<String> graphName;
    private final String instance = UUID.randomUUID().toString();
    private final Map<String, String> active = new HashMap<>();
    private final Map<String, String> released = new HashMap<>();
    private long epoch;
    private long revision;
    private volatile GraphMode mode = GraphMode.NONE;

    public LoadingMode(Supplier<String> graphName) {
        this.graphName = Objects.requireNonNull(graphName, "graphName");
    }

    public GraphMode mode() {
        return this.mode;
    }

    public synchronized void mode(GraphMode mode) {
        Objects.requireNonNull(mode, "mode");
        checkState(this.active.isEmpty(), "Loading owners are active; release them before setting graph mode");
        this.advanceRevision();
        this.mode = mode;
    }

    public synchronized Map<String, Object> capability() {
        return ImmutableMap.of("protocol", 1, "instance", this.instance, "epoch", this.epoch,
                               "revision", this.revision);
    }

    public synchronized Map<String, Object> recover(String instance, long epoch, long revision) {
        E.checkArgument(this.instance.equals(instance), "Loading owner belongs to another graph instance");
        E.checkArgument(epoch >= 0 && revision >= 0, "Invalid loading recovery epoch or revision");
        checkState(epoch == this.epoch && revision == this.revision,
                   "Loading state changed; inspect it before recovery");
        checkState(this.epoch < Long.MAX_VALUE, "Loading owner epoch exhausted");
        GraphMode previousMode = this.mode;
        int clearedOwners = this.active.size();
        this.advanceRevision();
        this.epoch++;
        this.active.clear();
        this.released.clear();
        this.mode = GraphMode.NONE;
        this.logChange("recover", previousMode, clearedOwners);
        return ImmutableMap.<String, Object>builder().putAll(this.capability())
                           .put("state", "recovered").put("mode", this.mode).build();
    }

    public synchronized Map<String, Object> update(String action, String instance, long epoch,
                                                   String token, String principal) {
        E.checkArgument("acquire".equals(action) || "release".equals(action),
                        "Loading owner action must be acquire or release");
        E.checkArgument(this.instance.equals(instance), "Loading owner belongs to another graph instance");
        E.checkArgument(epoch >= 0 && epoch <= this.epoch, "Invalid loading owner epoch");
        E.checkArgument(token != null && token.length() == 36 && UUID.fromString(token).toString().equals(token),
                        "Loading owner token must be a canonical UUID");
        Objects.requireNonNull(principal, "principal");
        GraphMode previousMode = this.mode;
        String key = epoch + "/" + token;
        String owner = this.active.get(key);
        if (owner != null) {
            checkState(owner.equals(principal), "Loading owner belongs to another authenticated principal");
            if ("acquire".equals(action)) {
                return this.receipt(epoch, token, "active");
            }
            // Exhaustion must fail before changing an active registration.
            checkState(epoch != this.epoch || this.released.size() < MAX_RELEASED ||
                         this.epoch < Long.MAX_VALUE, "Loading owner epoch exhausted");
            this.advanceRevision();
            this.active.remove(key);
            if (this.active.isEmpty()) {
                this.mode = GraphMode.NONE;
            }
        } else if ("acquire".equals(action)) {
            if (epoch < this.epoch || this.released.containsKey(key)) {
                return this.receipt(epoch, token, "released");
            }
            checkState(this.active.isEmpty() ? this.mode == GraphMode.NONE : this.mode == GraphMode.LOADING,
                         "Cannot acquire loading ownership of manually set graph mode %s", this.mode);
            checkState(this.active.size() < MAX_ACTIVE, "Loading owner limit %s reached", MAX_ACTIVE);
            this.advanceRevision();
            this.active.put(key, principal);
            this.mode = GraphMode.LOADING;
            this.logChange("acquire", previousMode, 0);
            return this.receipt(epoch, token, "active");
        }
        // Unknown release cancels an acquire which may still be in transit.
        if (epoch == this.epoch) {
            String releasedOwner = this.released.get(key);
            checkState(releasedOwner == null || releasedOwner.equals(principal),
                         "Loading owner belongs to another authenticated principal");
            if (releasedOwner != null) {
                return this.receipt(epoch, token, "released");
            }
            if (this.released.size() == MAX_RELEASED) {
                checkState(this.epoch < Long.MAX_VALUE, "Loading owner epoch exhausted");
                if (owner == null) {
                    this.advanceRevision();
                }
                this.epoch++;
                this.released.clear();
                // Old nonactive tuples are terminal; active old tuples remain registered.
            } else {
                if (owner == null) {
                    this.advanceRevision();
                }
                this.released.put(key, principal);
            }
        }
        if (owner != null) {
            this.logChange("release", previousMode, 1);
        }
        return this.receipt(epoch, token, "released");
    }

    private void logChange(String action, GraphMode previousMode, int clearedOwners) {
        LOG.info("Loading ownership {} for graph '{}' instance {}: mode {} -> {}, epoch {}, " +
                 "activeOwners {}, clearedOwners {}", action, this.graphName.get(), this.instance,
                 previousMode, this.mode, this.epoch, this.active.size(), clearedOwners);
    }

    private void advanceRevision() {
        checkState(this.revision < Long.MAX_VALUE, "Loading state revision exhausted");
        this.revision++;
    }

    private static void checkState(boolean condition, String message, Object... args) {
        if (!condition) {
            throw new ConflictException(String.format(message, args));
        }
    }

    public static class ConflictException extends IllegalStateException {

        public ConflictException(String message) {
            super(message);
        }
    }

    private Map<String, Object> receipt(long epoch, String token, String state) {
        return ImmutableMap.<String, Object>builder().put("protocol", 1).put("instance", this.instance)
                           .put("epoch", epoch).put("token", token).put("state", state)
                           .put("mode", this.mode).build();
    }
}
