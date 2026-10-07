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

package org.apache.hugegraph.query;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;

public final class IdPrefixQuery extends Query {

    private final Id start;
    private final boolean inclusiveStart;
    private final Id prefix;

    public IdPrefixQuery(HugeType resultType, Id prefix) {
        this(resultType, null, prefix, true, prefix);
    }

    public IdPrefixQuery(Query originQuery, Id prefix) {
        this(originQuery.resultType(), originQuery, prefix, true, prefix);
    }

    public IdPrefixQuery(Query originQuery, Id start, Id prefix) {
        this(originQuery.resultType(), originQuery, start, true, prefix);
    }

    public IdPrefixQuery(Query originQuery,
                         Id start, boolean inclusive, Id prefix) {
        this(originQuery.resultType(), originQuery, start, inclusive, prefix);
    }

    public IdPrefixQuery(HugeType resultType, Query originQuery,
                         Id start, boolean inclusive, Id prefix) {
        super(resultType, originQuery);
        E.checkArgumentNotNull(start, "The start parameter can't be null");
        this.start = start;
        this.inclusiveStart = inclusive;
        this.prefix = prefix;
        if (originQuery != null) {
            this.copyBasic(originQuery);
        }
    }

    public Id start() {
        return this.start;
    }

    public boolean inclusiveStart() {
        return this.inclusiveStart;
    }

    public Id prefix() {
        return this.prefix;
    }

    @Override
    public boolean empty() {
        return false;
    }

    @Override
    public boolean test(BaseElement element) {
        byte[] elem = element.id().asBytes();
        int cmp = Bytes.compare(elem, this.start.asBytes());
        boolean matchedStart = this.inclusiveStart ? cmp >= 0 : cmp > 0;
        boolean matchedPrefix = Bytes.prefixWith(elem, this.prefix.asBytes());
        return matchedStart && matchedPrefix;
    }

    @Override
    public IdPrefixQuery copy() {
        return (IdPrefixQuery) super.copy();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        assert sb.length() > 0;
        sb.deleteCharAt(sb.length() - 1); // Remove the last "`"
        sb.append(" id prefix with ").append(this.prefix);
        if (this.start != this.prefix) {
            sb.append(" and start with ").append(this.start)
              .append("(")
              .append(this.inclusiveStart ? "inclusive" : "exclusive")
              .append(")");
        }
        sb.append("`");
        return sb.toString();
    }
}
