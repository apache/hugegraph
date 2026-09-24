/*
 * Copyright 2017 HugeGraph Authors
 *
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

package org.apache.hugegraph.query;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.SchemaLabel;

public class MatchedIndex {

    private final SchemaLabel schemaLabel;
    private final Set<IndexLabel> indexLabels;

    public MatchedIndex(SchemaLabel schemaLabel,
                        Set<IndexLabel> indexLabels) {
        this.schemaLabel = schemaLabel;
        this.indexLabels = indexLabels;
    }

    public SchemaLabel schemaLabel() {
        return this.schemaLabel;
    }

    public Set<IndexLabel> indexLabels() {
        return Collections.unmodifiableSet(this.indexLabels);
    }


    public boolean containsSearchIndex() {
        for (IndexLabel il : this.indexLabels) {
            if (il.indexType().isSearch()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return indexLabels.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MatchedIndex)) {
            return false;
        }
        Set<IndexLabel> indexLabels = ((MatchedIndex) other).indexLabels;
        return Objects.equals(this.indexLabels, indexLabels);
    }

    @Override
    public String toString() {
        String strIndexLabels =
                indexLabels.stream().map(i -> i.name()).collect(Collectors.joining(","));

        return "MatchedIndex{schemaLabel=" + schemaLabel.name() +
               ", indexLabels=" + strIndexLabels + '}';
    }
}
