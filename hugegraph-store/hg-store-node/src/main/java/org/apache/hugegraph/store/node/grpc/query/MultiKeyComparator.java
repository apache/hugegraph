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

package org.apache.hugegraph.store.node.grpc.query;

import java.util.Comparator;
import java.util.List;

import org.apache.hugegraph.store.util.MultiKv;

public class MultiKeyComparator implements Comparator<MultiKv> {

    private final List<Integer> orders;

    public MultiKeyComparator(List<Integer> orders) {
        this.orders = orders;
    }

    @Override
    public int compare(MultiKv o1, MultiKv o2) {
        var key1 = o1 == null ? null : o1.getKeys();
        var key2 = o2 == null ? null : o2.getKeys();

        if (key1 == null || key2 == null) {
            if (key1 == null && key2 == null) {
                return 0;
            }
            return key1 == null ? -1 : 1;
        }

        for (int i = 0; i < this.orders.size(); i++) {
            var index = this.orders.get(i);
            var v1 = key1.size() > index ? key1.get(index) : null;
            var v2 = key2.size() > index ? key2.get(index) : null;
            int ret = compareV((Comparable) v1, (Comparable) v2);
            if (ret != 0) {
                return ret;
            }
        }

        return key1.size() - key2.size();
    }

    private int compareV(Comparable a, Comparable b) {
        if (a == null || b == null) {
            if (a == null && b == null) {
                return 0;
            }

            return a == null ? -1 : 1;
        }

        return a.compareTo(b);
    }
}
