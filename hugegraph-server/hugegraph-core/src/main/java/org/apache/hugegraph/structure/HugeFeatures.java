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

package org.apache.hugegraph.structure;

import java.util.UUID;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class HugeFeatures implements Graph.Features {

    protected final HugeGraph graph;
    protected final boolean supportsPersistence;
    protected final HugeGraphFeatures graphFeatures;
    protected final HugeVertexFeatures vertexFeatures;
    protected final HugeEdgeFeatures edgeFeatures;

    public HugeFeatures(HugeGraph graph, boolean supportsPersistence) {
        this.graph = graph;
        this.supportsPersistence = supportsPersistence;

        this.graphFeatures = new HugeGraphFeatures();
        this.vertexFeatures = new HugeVertexFeatures();
        this.edgeFeatures = new HugeEdgeFeatures();
    }

    @Override
    public HugeGraphFeatures graph() {
        return this.graphFeatures;
    }

    @Override
    public HugeVertexFeatures vertex() {
        return this.vertexFeatures;
    }

    @Override
    public HugeEdgeFeatures edge() {
        return this.edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public class HugeGraphFeatures implements GraphFeatures {

        private final VariableFeatures variableFeatures =
                new HugeVariableFeatures();

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return HugeFeatures.this.supportsPersistence;
        }

        @Override
        public VariableFeatures variables() {
            return this.variableFeatures;
        }

        @Override
        public boolean supportsTransactions() {
            return true;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    public class HugeElementFeatures implements ElementFeatures {

        @Override
        public boolean supportsAddProperty() {
            return true;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return true;
        }

        @Override
        public boolean supportsStringIds() {
            return true;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean willAllowId(Object id) {
            if (!this.supportsUserSuppliedIds()) {
                return false;
            } else {
                return this.supportsAnyIds() ||
                       this.supportsCustomIds() && id instanceof Id ||
                       this.supportsStringIds() && id instanceof String ||
                       this.supportsNumericIds() && id instanceof Number ||
                       this.supportsUuidIds() && id instanceof UUID;
            }
        }
    }

    public class HugeVariableFeatures extends HugeDataTypeFeatures
            implements VariableFeatures {

    }

    public class HugeVertexPropertyFeatures extends HugeDataTypeFeatures
            implements VertexPropertyFeatures {

        @Override
        public boolean supportsRemoveProperty() {
            return true;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public class HugeEdgePropertyFeatures extends HugeDataTypeFeatures
            implements EdgePropertyFeatures {

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }

    }

    public class HugeVertexFeatures extends HugeElementFeatures
            implements VertexFeatures {

        private final VertexPropertyFeatures vertexPropertyFeatures =
                new HugeVertexPropertyFeatures();

        @Override
        public boolean supportsUserSuppliedIds() {
            return true;
        }

        @Override
        public VertexPropertyFeatures properties() {
            return this.vertexPropertyFeatures;
        }

        @Override
        public boolean supportsMultiProperties() {
            // Regard as a set (actually can also be a list)
            return true;
        }

        @Override
        public boolean supportsDuplicateMultiProperties() {
            // Regard as a list
            return true;
        }

        @Override
        public boolean supportsMetaProperties() {
            // Nested property
            return false;
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }

        public boolean supportsDefaultLabel() {
            return true;
        }

        public String defaultLabel() {
            return HugeFeatures.this.graph
                    .option(CoreOptions.VERTEX_DEFAULT_LABEL);
        }
    }

    public class HugeEdgeFeatures extends HugeElementFeatures
            implements EdgeFeatures {

        private final EdgePropertyFeatures edgePropertyFeatures =
                new HugeEdgePropertyFeatures();

        @Override
        public EdgePropertyFeatures properties() {
            return this.edgePropertyFeatures;
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }
    }

    public class HugeDataTypeFeatures implements DataTypeFeatures {

        @Override
        @FeatureDescriptor(name = FEATURE_STRING_VALUES)
        public boolean supportsStringValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
        public boolean supportsBooleanValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
        public boolean supportsByteValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
        public boolean supportsFloatValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
        public boolean supportsDoubleValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
        public boolean supportsIntegerValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_LONG_VALUES)
        public boolean supportsLongValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
        public boolean supportsUniformListValues() {
            /*
             * NOTE: must use cardinality list if use LIST property value,
             * can't support a LIST property value with cardinality single
             */
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_MAP_VALUES)
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
        public boolean supportsSerializableValues() {
            return false;
        }

        /**
         * All these supportsXxArrayValues() must be used with cardinality list
         * we can't support array values with cardinality single like tinkerpop
         */
        @Override
        @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
        public boolean supportsByteArrayValues() {
            // Regard as blob
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
        public boolean supportsStringArrayValues() {
            return false;
        }
    }
}
