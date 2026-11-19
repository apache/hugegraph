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

package org.apache.hugegraph.pd.raft;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.ThreadId;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftReflectionUtil {

    /**
     * The last index of the node's raft log, read from its log manager; Long.MAX_VALUE when
     * it cannot be read, which the caller treats as "unknown"
     */
    /**
     * The log manager of the node, read once; null when it cannot be read. It reads the private
     * field NodeImpl.logManager of jraft; RaftReflectionUtilTest pins that to the jraft version
     * of the pom, so a jraft upgrade that renames or retypes the field fails CI rather than
     * leaving every replay unchecked in production.
     */
    public static LogManager getLogManager(Node node) {
        if (node == null) {
            return null;
        }
        try {
            var f = node.getClass().getDeclaredField("logManager");
            f.setAccessible(true);
            try {
                return (LogManager) f.get(node);
            } finally {
                f.setAccessible(false);
            }
        } catch (NoSuchFieldException | IllegalAccessException | ClassCastException e) {
            log.warn("Failed to get the raft log manager via reflection: {}", e.getMessage(), e);
            return null;
        }
    }

    public static Replicator.State getReplicatorState(Node node, PeerId peerId) {
        if (node == null || peerId == null) {
            return null;
        }

        // Get ReplicatorGroup from Node
        var clz = node.getClass();
        ReplicatorGroup replicateGroup = null;
        try {
            var f = clz.getDeclaredField("replicatorGroup");
            f.setAccessible(true);
            try {
                replicateGroup = (ReplicatorGroup)f.get(node);
            }
            finally {
                f.setAccessible(false);
            }
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            log.warn("Failed to get replicator state via reflection: {}", e.getMessage(), e);
            return null;
        }

        if (replicateGroup == null) {
            return null;
        }

        ThreadId threadId = replicateGroup.getReplicator(peerId);
        if (threadId == null) {
            return null;
        }
        else {
            Replicator r = (Replicator)threadId.lock();
            try {
                if (r == null) {
                    return Replicator.State.Probe;
                }
                Replicator.State result = null;

                // Get state from Replicator

                var replicatorClz = r.getClass();
                try {
                    var f = replicatorClz.getDeclaredField("state");
                    f.setAccessible(true);
                    try {
                        result = (Replicator.State)f.get(r);
                    }catch (Exception e){
                        log.warn("Failed to get replicator state for peerId: {}, error: {}", peerId, e.getMessage());
                    }
                    finally {
                        f.setAccessible(false);
                    }
                }
                catch (NoSuchFieldException e) {
                    log.warn("Failed to get replicator state via reflection: {}", e.getMessage(), e);
                    result = null;
                }
                return result;
            } finally {
                threadId.unlock();
            }
        }
    }
}
