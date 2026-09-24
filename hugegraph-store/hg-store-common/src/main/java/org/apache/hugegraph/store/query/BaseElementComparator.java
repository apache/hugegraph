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

package org.apache.hugegraph.store.query;

import java.util.Comparator;
import java.util.List;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.structure.BaseElement;

public class BaseElementComparator implements Comparator<BaseElement> {

    private final List<Id> ids;

    private boolean isAsc;

    public BaseElementComparator(List<Id> list, boolean isAsc) {
        this.ids = list;
        this.isAsc = isAsc;
    }

    public void reverseOrder() {
        this.isAsc = !this.isAsc;
    }

    @Override
    public int compare(BaseElement o1, BaseElement o2) {
        if (o1 == null || o2 == null) {
            if (o1 == null && o2 == null) {
                return 0;
            }
            return (o1 == null ? -1 : 1) * (this.isAsc ? 1 : -1);
        }
        //FIXME may cause NPE exception
        for (Id id : ids) {
            var ret = compareProperty(o1.getPropertyValue(id), o2.getPropertyValue(id));
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }

    private int compareProperty(Comparable a, Comparable b) {

        if (a != null && b != null) {
            return (a.compareTo(b)) * (this.isAsc ? 1 : -1);
        }

        if (a == null && b == null) {
            return 0;
        }

        return (a == null ? -1 : 1) * (this.isAsc ? 1 : -1);
    }
}
