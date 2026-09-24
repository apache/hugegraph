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

package org.apache.hugegraph.store.business;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.Graphpb;
import org.apache.hugegraph.store.grpc.Graphpb.Edge;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.Request;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.ScanType;
import org.apache.hugegraph.store.grpc.Graphpb.Variant.Builder;
import org.apache.hugegraph.store.grpc.Graphpb.VariantType;
import org.apache.hugegraph.store.grpc.Graphpb.Vertex;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseProperty;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Blob;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;

import groovy.lang.MissingMethodException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphStoreIterator<T> extends AbstractSelectIterator
        implements ScanIterator {

    private static final Descriptors.FieldDescriptor propertiesDesEdge =
            Edge.getDescriptor().findFieldByNumber(6);
    private static final Descriptors.FieldDescriptor propertiesDesVertex =
            Vertex.getDescriptor().findFieldByNumber(3);
    private final ScanPartitionRequest scanRequest;
    private final ScanIterator iter;
    private final Request request;
    private final boolean isVertex;
    private final HugeType type;
    private final Set properties;
    private Vertex.Builder vertex;
    private Edge.Builder edge;
    private ArrayList<RocksDBSession.BackendColumn> data;
    private GroovyScriptEngineImpl engine;
    private CompiledScript script;
    private BaseElement current;
    private Exception stopCause;

    public GraphStoreIterator(ScanIterator iterator,
                              ScanPartitionRequest scanRequest) {
        super();
        this.iter = iterator;
        this.scanRequest = scanRequest;
        this.request = this.scanRequest.getScanRequest();
        ScanType scanType = this.request.getScanType();
        isVertex = scanType.equals(ScanType.SCAN_VERTEX);
        if (isVertex) {
            vertex = Vertex.newBuilder();
            type = HugeType.VERTEX;
        } else {
            edge = Edge.newBuilder();
            type = HugeType.EDGE;
        }
        properties = new HashSet<Long>();
        List<Long> pl = request.getPropertiesList();
        if (pl != null) {
            for (Long i : pl) {
                properties.add(i);
            }
        }
        String condition = request.getCondition();
        if (!StringUtils.isEmpty(condition)) {
            ScriptEngineManager factory = new ScriptEngineManager();
            engine = (GroovyScriptEngineImpl) factory.getEngineByName("groovy");
            try {
                script = engine.compile(condition);
            } catch (ScriptException e) {
                log.error("create script with error:", e);
            }
        }
    }

    private BaseElement getElement(RocksDBSession.BackendColumn next) {
        return this.parseEntry(BackendColumn.of(next.name, next.value), isVertex);
    }

    @Override
    public boolean hasNext() {
        if (current == null) {
            while (iter.hasNext()) {
                RocksDBSession.BackendColumn next = this.iter.next();
                BaseElement element = getElement(next);
                try {
                    boolean evalResult = true;
                    if (isVertex) {
                        BaseVertex el = (BaseVertex) element;
                        if (engine != null) {
                            Bindings bindings = engine.createBindings();
                            bindings.put("element", el);
                            evalResult = (boolean) script.eval(bindings);
                        }
                    } else {
                        BaseEdge el = (BaseEdge) element;
                        if (engine != null) {
                            Bindings bindings = engine.createBindings();
                            bindings.put("element", el);
                            evalResult = (boolean) script.eval(bindings);
                        }
                    }
                    if (!evalResult) {
                        continue;
                    }
                    current = element;
                    return true;
                } catch (ScriptException | MissingMethodException se) {
                    stopCause = se;
                    log.error("get next with error which cause to stop:", se);
                    return false;
                } catch (Exception e) {
                    log.error("get next with error:", e);
                }
            }
        } else {
            return true;
        }
        return false;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public T next() {
        T next;
        if (isVertex) {
            next = (T) parseVertex(current);
        } else {
            next = (T) parseEdge(current);
        }
        current = null;
        return next;
    }

    public T select(RocksDBSession.BackendColumn current) {
        BaseElement element = getElement(current);
        if (isVertex) {
            return (T) parseVertex(element);
        } else {
            return (T) parseEdge(element);
        }
    }

    public ArrayList<T> convert() {
        ArrayList result = new ArrayList(data.size());
        for (int i = 0; i < data.size(); i++) {
            result.add(select(data.get(i)));
        }
        return result;
    }

    private <P extends BaseProperty<?>> List<Graphpb.Property> buildProperties(
            Builder variant,
            int size,
            Iterator<P> eps) {
        int pSize = properties.size();
        List<Graphpb.Property> props = new ArrayList<>(pSize > 0 ?
                                                       pSize : size);
        Graphpb.Property.Builder pb = Graphpb.Property.newBuilder();
        while (eps.hasNext()) {
            BaseProperty<?> property = eps.next();
            PropertyKey key = property.propertyKey();
            long pkId = key.id().asLong();
            if (pSize > 0 && !properties.contains(pkId)) {
                continue;
            }
            pb.clear();
            variant.clear();
            pb.setLabel(pkId);
            Object v = property.value();
            switch (key.dataType()) {
                case UUID:
                    variant.setType(VariantType.VT_STRING)
                           .setValueString(v.toString());
                    break;
                case LONG:
                    variant.setType(VariantType.VT_LONG)
                           .setValueInt64((Long) v);
                    break;
                case INT:
                    variant.setType(VariantType.VT_INT)
                           .setValueInt32((Integer) v);
                    break;
                case BLOB:
                    byte[] bytes = v instanceof byte[] ?
                                   (byte[]) v : ((Blob) v).bytes();
                    variant.setType(VariantType.VT_BYTES)
                           .setValueBytes(ByteString.copyFrom(bytes));
                    break;
                case BYTE:
                    variant.setType(VariantType.VT_BYTES)
                           .setValueBytes(
                                   ByteString.copyFrom(new byte[]{(Byte) v}));
                    break;
                case DATE:
                    Date date = (Date) v;
                    variant.setType(VariantType.VT_DATETIME)
                           .setValueDatetime(date.toString());
                    break;
                case FLOAT:
                    variant.setType(VariantType.VT_FLOAT)
                           .setValueFloat((Float) v);
                    break;
                case TEXT:
                    variant.setType(VariantType.VT_STRING)
                           .setValueString((String) v);
                    break;
                case DOUBLE:
                    variant.setType(VariantType.VT_DOUBLE)
                           .setValueDouble((Double) v);
                    break;
                case OBJECT:
                case UNKNOWN:
                    variant.setType(VariantType.VT_UNKNOWN)
                           .setValueString(v.toString());
                    break;
                case BOOLEAN:
                    variant.setType(VariantType.VT_BOOLEAN)
                           .setValueBoolean((Boolean) v);
                    break;
                default:
                    break;
            }
            pb.setValue(variant.build());
            props.add(pb.build());
        }
        return props;
    }

    private void buildId(Builder variant, Id id) {
        switch (id.type()) {
            case STRING:
            case UUID:
                variant.setType(VariantType.VT_STRING)
                       .setValueString(id.asString());
                break;
            case LONG:
                variant.setType(VariantType.VT_LONG)
                       .setValueInt64(id.asLong());
                break;
            case EDGE:
                // TODO
                break;
            case UNKNOWN:
                variant.setType(VariantType.VT_UNKNOWN)
                       .setValueBytes(ByteString.copyFrom(id.asBytes()));
                break;

            default:
                break;

        }
    }

    private Edge parseEdge(BaseElement element) {
        BaseEdge e = (BaseEdge) element;
        edge.clear();
        EdgeLabel label = e.schemaLabel();
        edge.setLabel(label.longId());
        edge.setSourceLabel(e.sourceVertex().schemaLabel().id().asLong());
        edge.setTargetLabel(e.targetVertex().schemaLabel().id().asLong());
        Builder variant = Graphpb.Variant.newBuilder();
        buildId(variant, e.sourceVertex().id());
        edge.setSourceId(variant.build());
        variant.clear();
        buildId(variant, e.targetVertex().id());
        edge.setTargetId(variant.build());
        int size = e.sizeOfProperties();
        Iterator<BaseProperty<?>> eps = e.properties().iterator();
        List<Graphpb.Property> props = buildProperties(variant, size, eps);
        edge.setField(propertiesDesEdge, props);
        return edge.build();
    }

    private Vertex parseVertex(BaseElement element) {
        BaseVertex v = (BaseVertex) element;
        vertex.clear();
        VertexLabel label = v.schemaLabel();
        vertex.setLabel(label.longId());
        Builder variant = Graphpb.Variant.newBuilder();
        buildId(variant, v.id());
        vertex.setId(variant.build());
        int size = v.sizeOfProperties();
        Iterator<BaseProperty<?>> vps = v.properties().iterator();
        List<Graphpb.Property> props = buildProperties(variant, size, vps);
        vertex.setField(propertiesDesVertex, props);
        return vertex.build();
    }

    @Override
    public void close() {
        iter.close();
    }

    public Exception getStopCause() {
        return stopCause;
    }
}
