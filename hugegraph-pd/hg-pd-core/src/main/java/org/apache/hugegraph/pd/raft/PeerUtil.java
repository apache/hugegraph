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

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.entity.PeerId;
import org.apache.hugegraph.pd.common.KVPair;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class PeerUtil {
    public static boolean isPeerEquals(PeerId p1, PeerId p2) {
        if (p1 == null && p2 == null) {
            return true;
        }
        if (p1 == null || p2 == null) {
            return false;
        }
        return Objects.equals(p1.getIp(), p2.getIp()) && Objects.equals(p1.getPort(), p2.getPort());
    }

    public static List<KVPair<String, PeerId>> parseConfig(String conf) {
        List<KVPair<String, PeerId>> result = new LinkedList<>();

        if (conf != null && conf.length() > 0) {
            for (var s : conf.split(",")) {
                if (s.endsWith("/leader")) {
                    result.add(new KVPair<>("leader", JRaftUtils.getPeerId(s.substring(0, s.length() - 7))));
                } else if (s.endsWith("/learner")) {
                    result.add(new KVPair<>("learner", JRaftUtils.getPeerId(s.substring(0, s.length() - 8))));
                } else if (s.endsWith("/follower")) {
                    result.add(new KVPair<>("follower", JRaftUtils.getPeerId(s.substring(0, s.length() - 9))));
                } else {
                    result.add(new KVPair<>("follower", JRaftUtils.getPeerId(s)));
                }
            }
        }

        return result;
    }
}
