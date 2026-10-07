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

package org.apache.hugegraph.job.schema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.job.SysJob;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public abstract class SchemaJob extends SysJob<Object> {

    public static final String REMOVE_SCHEMA = "remove_schema";
    public static final String REBUILD_INDEX = "rebuild_index";
    public static final String CREATE_INDEX = "create_index";
    public static final String CREATE_OLAP = "create_olap";
    public static final String CLEAR_OLAP = "clear_olap";
    public static final String REMOVE_OLAP = "remove_olap";

    protected static final Logger LOG = Log.logger(SchemaJob.class);

    private static final String SPLITOR = ":";

    protected HugeType schemaType() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR, 3);
        E.checkState(parts.length == 3 && parts[0] != null,
                     "Task name should be formatted to String " +
                     "'TYPE:ID:NAME', but got '%s'", name);

        return HugeType.valueOf(parts[0]);
    }

    protected Id schemaId() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR, 3);
        E.checkState(parts.length == 3 && parts[1] != null,
                     "Task name should be formatted to String " +
                     "'TYPE:ID:NAME', but got '%s'", name);
        return IdGenerator.of(Long.valueOf(parts[1]));
    }

    protected String schemaName() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR, 3);
        E.checkState(parts.length == 3 && parts[2] != null,
                     "Task name should be formatted to String " +
                     "'TYPE:ID:NAME', but got '%s'", name);
        return parts[2];
    }

    public static String formatTaskName(HugeType type, Id id, String name) {
        E.checkNotNull(type, "schema type");
        E.checkNotNull(id, "schema id");
        E.checkNotNull(name, "schema name");
        return String.join(SPLITOR, type.toString(), id.asString(), name);
    }

    /**
     * Use reflection to call SchemaTransaction.removeSchema(),
     * which is protected
     *
     * @param tx     The remove operation actual executer
     * @param schema the schema to be removed
     */
    protected static void removeSchema(ISchemaTransaction tx,
                                       SchemaElement schema) {
        try {
            Method method = ISchemaTransaction.class
                    .getDeclaredMethod("removeSchema",
                                       SchemaElement.class);
            method.setAccessible(true);
            method.invoke(tx, schema);
        } catch (NoSuchMethodException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new AssertionError(
                    "Can't call SchemaTransaction.removeSchema()", e);
        }

    }

    /**
     * Use reflection to call SchemaTransaction.updateSchema(),
     * which is protected
     *
     * @param tx     The update operation actual execute
     * @param schema the schema to be updated
     */
    protected static void updateSchema(ISchemaTransaction tx,
                                       SchemaElement schema) {
        try {
            Method method = ISchemaTransaction.class
                    .getDeclaredMethod("updateSchema",
                                       SchemaElement.class);
            method.setAccessible(true);
            method.invoke(tx, schema);
        } catch (NoSuchMethodException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new AssertionError(
                    "Can't call SchemaTransaction.updateSchema()", e);
        }
    }
}
