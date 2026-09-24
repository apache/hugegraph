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

package org.apache.hugegraph.store.util;

import java.io.Serializable;
import java.util.List;

import lombok.Data;

@Data
public class MultiKv implements Comparable<MultiKv>, Serializable {

    private List<Object> keys;
    private List<Object> values;

    private List<Long> compareIndex;

    public MultiKv(List<Object> keys, List<Object> values) {
        this.keys = keys;
        this.values = values;
    }

    public static MultiKv of(List<Object> keys, List<Object> values) {
        return new MultiKv(keys, values);
    }

    @Override
    public int compareTo(MultiKv o) {
        if (keys == null && o == null) {
            return 0;
        }
        if (keys == null) {
            return -1;
        } else if (o.keys == null) {
            return 1;
        } else {
            int l1 = keys.size();
            int l2 = o.getKeys().size();
            for (int i = 0; i < Math.min(l1, l2); i++) {
                if (keys.get(i) instanceof Comparable && o.getKeys().get(i) instanceof Comparable) {
                    var ret = ((Comparable) keys.get(i)).compareTo(o.getKeys().get(i));
                    if (ret != 0) {
                        return ret;
                    }
                } else {
                    return 1;
                }
            }

            if (l1 != l2) {
                return l1 > l2 ? 1 : -1;
            }
        }
        return 0;
    }
}
