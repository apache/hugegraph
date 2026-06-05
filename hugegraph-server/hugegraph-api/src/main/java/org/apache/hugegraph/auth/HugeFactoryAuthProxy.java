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

package org.apache.hugegraph.auth;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration2.Configuration;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.backend.cache.CacheManager;
import org.apache.hugegraph.backend.tx.AbstractTransaction;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.backend.tx.IndexableTransaction;
import org.apache.hugegraph.concurrent.LockManager;
import org.apache.hugegraph.metrics.ServerReporter;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.builder.AbstractBuilder;
import org.apache.hugegraph.schema.builder.EdgeLabelBuilder;
import org.apache.hugegraph.schema.builder.IndexLabelBuilder;
import org.apache.hugegraph.schema.builder.PropertyKeyBuilder;
import org.apache.hugegraph.schema.builder.VertexLabelBuilder;
import org.apache.hugegraph.serializer.JsonSerializer;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.StandardTaskScheduler;
import org.apache.hugegraph.task.TaskCallable;
import org.apache.hugegraph.task.TaskCallable.SysTaskCallable;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.traversal.optimize.HugeCountStepStrategy;
import org.apache.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import org.apache.hugegraph.traversal.optimize.HugeVertexStepStrategy;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.Reflection;
import org.apache.hugegraph.variables.HugeVariables;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;

public final class HugeFactoryAuthProxy {

    public static final String GRAPH_FACTORY =
            "gremlin.graph=org.apache.hugegraph.auth.HugeFactoryAuthProxy";
    private static final Logger LOG = Log.logger(HugeFactoryAuthProxy.class);
    private static final Set<String> PROTECT_METHODS = ImmutableSet.of("instance");

    private static final Map<HugeGraph, HugeGraph> GRAPHS = new HashMap<>();

    static {
        HugeGraphAuthProxy.setContext(HugeGraphAuthProxy.Context.admin());
        HugeFactoryAuthProxy.registerPrivateActions();
    }

    public static synchronized HugeGraph open(Configuration config) {
        /*
         * Inject authentication (replace HugeGraph with HugeGraphAuthProxy)
         * TODO: Add verify to StandardHugeGraph() to prevent dynamic creation
         */
        HugeGraph graph = HugeFactory.open(config);
        HugeGraph proxy = GRAPHS.get(graph);
        if (proxy == null) {
            proxy = new HugeGraphAuthProxy(graph);
            GRAPHS.put(graph, proxy);
        }
        return proxy;
    }

    // TODO: add some test to ensure the effect & partially move to HugeSecurityManager
    private static void registerPrivateActions() {
        // Sensitive classes (Be careful to add classes here due to JDK compatibility)
        filterCriticalSystemClasses();

        // Thread
        Reflection.registerFieldsToFilter(java.lang.Thread.class, "name", "priority", "threadQ",
                                          "eetop", "single_step", "daemon", "stillborn", "target",
                                          "group", "contextClassLoader",
                                          "inheritedAccessControlContext", "threadInitNumber",
                                          "threadLocals", "inheritableThreadLocals", "stackSize",
                                          "nativeParkEventPointer", "tid", "threadSeqNumber",
                                          "threadStatus", "parkBlocker", "blocker", "blockerLock",
                                          "EMPTY_STACK_TRACE", "SUBCLASS_IMPLEMENTATION_PERMISSION",
                                          "uncaughtExceptionHandler",
                                          "defaultUncaughtExceptionHandler",
                                          "threadLocalRandomSeed",
                                          "threadLocalRandomSecondarySeed");
        Reflection.registerMethodsToFilter(java.lang.Thread.class, "exit",
                                           "dispatchUncaughtException", "clone", "isInterrupted",
                                           "registerNatives", "init", "nextThreadNum",
                                           "nextThreadID", "blockedOn", "start0", "isCCLOverridden",
                                           "auditSubclass", "dumpThreads", "getThreads",
                                           "processQueue", "setPriority0", "stop0", "suspend0",
                                           "resume0", "interrupt0", "setNativeName");
        Reflection.registerFieldsToFilter(java.lang.ThreadLocal.class, "threadLocalHashCode",
                                          "nextHashCode", "HASH_INCREMENT");
        Reflection.registerMethodsToFilter(java.lang.ThreadLocal.class, "access$400",
                                           "createInheritedMap", "nextHashCode", "initialValue",
                                           "setInitialValue", "getMap", "createMap", "childValue");
        Reflection.registerMethodsToFilter(java.lang.InheritableThreadLocal.class, "getMap",
                                           "createMap", "childValue");

        // HugeGraph
        Reflection.registerFieldsToFilter(StandardAuthenticator.class, "graph");
        Reflection.registerMethodsToFilter(StandardAuthenticator.class, "initAdminUser",
                                           "inputPassword", "graph");
        Reflection.registerFieldsToFilter(HugeFactoryAuthProxy.class, "PROTECT_METHODS");
        Reflection.registerMethodsToFilter(HugeFactoryAuthProxy.class, "genRegisterPrivateActions",
                                           "registerClass", "registerPrivateActions",
                                           "registerPrivateActions", "c");
        Reflection.registerFieldsToFilter(HugeAuthenticator.User.class, "role", "client");
        Reflection.registerFieldsToFilter(
                org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser.class, "name");
        Reflection.registerFieldsToFilter(HugeGraphAuthProxy.class, "LOG", "hugegraph",
                                          "taskScheduler", "authManager", "contexts",
                                          "$assertionsDisabled");
        Reflection.registerMethodsToFilter(HugeGraphAuthProxy.class, "lambda$0", "access$3",
                                           "access$4", "access$2", "access$5", "resetContext",
                                           "logUser", "verifyAdminPermission",
                                           "verifyStatusPermission", "verifyPermission",
                                           "verifySchemaPermission", "verifySchemaPermission",
                                           "verifySchemaPermission", "verifySchemaPermission",
                                           "verifyNamePermission", "verifyNameExistsPermission",
                                           "verifyElemPermission", "verifyElemPermission",
                                           "verifyElemPermission", "verifyElemPermission",
                                           "verifyResPermission", "verifyResPermission",
                                           "verifyUserPermission", "verifyUserPermission",
                                           "verifyUserPermission", "getContextString", "access$6",
                                           "access$7", "lambda$1", "lambda$2", "lambda$3",
                                           "lambda$4", "lambda$5", "lambda$6", "lambda$7",
                                           "lambda$8", "lambda$9", "lambda$10", "lambda$11",
                                           "lambda$12", "lambda$13", "lambda$14", "lambda$15",
                                           "lambda$16", "lambda$17", "lambda$18", "lambda$19",
                                           "lambda$20", "lambda$21", "lambda$22", "lambda$23",
                                           "lambda$24", "access$8", "access$9", "access$10",
                                           "setContext", "getContext");
        Reflection.registerFieldsToFilter(HugeGraphAuthProxy.AuthManagerProxy.class, "authManager",
                                          "this$0");
        Reflection.registerMethodsToFilter(HugeGraphAuthProxy.AuthManagerProxy.class,
                                           "currentUsername", "updateCreator");
        Reflection.registerFieldsToFilter(HugeGraphAuthProxy.TaskSchedulerProxy.class,
                                          "taskScheduler", "this$0");
        Reflection.registerMethodsToFilter(HugeGraphAuthProxy.TaskSchedulerProxy.class, "lambda$0",
                                           "lambda$1", "lambda$2", "verifyTaskPermission",
                                           "verifyTaskPermission", "verifyTaskPermission",
                                           "verifyTaskPermission", "hasTaskPermission");
        Reflection.registerFieldsToFilter(HugeGraphAuthProxy.GraphTraversalSourceProxy.class,
                                          "this$0");
        Reflection.registerFieldsToFilter(
                org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.class,
                "connection", "graph", "strategies", "bytecode");
        Reflection.registerFieldsToFilter(HugeGraphAuthProxy.TraversalStrategiesProxy.class,
                                          "REST_WOEKER", "serialVersionUID", "strategies",
                                          "this$0");
        Reflection.registerMethodsToFilter(HugeGraphAuthProxy.TraversalStrategiesProxy.class,
                                           "translate");
        Reflection.registerFieldsToFilter(HugeGraphAuthProxy.VariablesProxy.class, "variables",
                                          "this$0");
        Reflection.registerFieldsToFilter(HugeGraphAuthProxy.Context.class, "ADMIN", "user");
        Reflection.registerFieldsToFilter(HugeGraphAuthProxy.ContextTask.class, "runner",
                                          "context");
        Reflection.registerFieldsToFilter(StandardHugeGraph.class, "LOG", "started", "closed",
                                          "mode", "variables", "name", "params", "configuration",
                                          "schemaEventHub", "graphEventHub", "indexEventHub",
                                          "writeRateLimiter", "readRateLimiter", "taskManager",
                                          "authManager", "features", "storeProvider", "tx",
                                          "ramtable", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(StandardHugeGraph.class, "lambda$0", "access$3",
                                           "access$4", "access$2", "access$5", "access$6",
                                           "access$7", "waitUntilAllTasksCompleted", "access$8",
                                           "loadStoreProvider", "graphTransaction",
                                           "schemaTransaction", "openSchemaTransaction",
                                           "checkGraphNotClosed", "openSystemTransaction",
                                           "openGraphTransaction", "systemTransaction", "access$9",
                                           "access$10", "access$11", "access$12", "access$13",
                                           "access$14", "access$15", "access$16", "access$17",
                                           "access$18", "serializer", "loadSchemaStore",
                                           "loadSystemStore", "loadGraphStore", "closeTx",
                                           "analyzer", "serverInfoManager", "reloadRamtable",
                                           "reloadRamtable", "access$19", "access$20", "access$21");
        Reflection.registerFieldsToFilter(
                loadClass("org.apache.hugegraph.StandardHugeGraph$StandardHugeGraphParams"),
                "graph", "this$0");
        Reflection.registerMethodsToFilter(
                loadClass("org.apache.hugegraph.StandardHugeGraph$StandardHugeGraphParams"),
                "access$1", "graph");
        Reflection.registerFieldsToFilter(
                loadClass("org.apache.hugegraph.StandardHugeGraph$TinkerPopTransaction"), "refs",
                "opened", "transactions", "this$0", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(
                loadClass("org.apache.hugegraph.StandardHugeGraph$TinkerPopTransaction"),
                "lambda$0", "access$3", "access$2", "lambda$1", "graphTransaction",
                "schemaTransaction", "systemTransaction", "access$1", "setOpened", "doCommit",
                "verifyOpened", "doRollback", "doClose", "destroyTransaction", "doOpen",
                "setClosed", "getOrNewTransaction", "access$0", "resetState");
        Reflection.registerFieldsToFilter(
                org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction.class,
                "readWriteConsumerInternal", "closeConsumerInternal", "transactionListeners");
        Reflection.registerMethodsToFilter(
                org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction.class,
                "doClose", "fireOnCommit", "fireOnRollback", "doReadWrite",
                "lambda$fireOnRollback$1", "lambda$fireOnCommit$0");
        Reflection.registerFieldsToFilter(
                org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction.class, "g");
        Reflection.registerMethodsToFilter(
                org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction.class, "doCommit",
                "doRollback", "doClose", "doOpen", "fireOnCommit", "fireOnRollback", "doReadWrite");
        Reflection.registerFieldsToFilter(loadClass("org.apache.hugegraph.StandardHugeGraph$Txs"),
                                          "schemaTx", "systemTx", "graphTx", "openedTime",
                                          "$assertionsDisabled");
        Reflection.registerMethodsToFilter(loadClass("org.apache.hugegraph.StandardHugeGraph$Txs"),
                                           "access$2", "access$1", "access$0");
        Reflection.registerFieldsToFilter(GraphTransaction.class, "indexTx", "addedVertices",
                                          "removedVertices", "addedEdges", "removedEdges",
                                          "addedProps", "removedProps", "updatedVertices",
                                          "updatedEdges", "updatedOldestProps", "locksTable",
                                          "checkCustomVertexExist", "checkAdjacentVertexExist",
                                          "lazyLoadAdjacentVertex", "ignoreInvalidEntry",
                                          "commitPartOfAdjacentEdges", "batchSize", "pageSize",
                                          "verticesCapacity", "edgesCapacity",
                                          "$assertionsDisabled",
                                          "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy");
        Reflection.registerMethodsToFilter(GraphTransaction.class, "lambda$0", "lambda$1",
                                           "lambda$2", "lambda$3", "lambda$4", "lambda$5",
                                           "lambda$6", "lambda$7", "lambda$8", "lambda$9",
                                           "lambda$10", "lambda$11", "lambda$12", "lambda$13",
                                           "lambda$14", "lambda$15", "lambda$16", "lambda$17",
                                           "lambda$18", "lambda$19", "access$1",
                                           "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy",
                                           "indexTransaction", "indexTransaction", "beforeWrite",
                                           "prepareCommit", "verticesInTxSize", "edgesInTxSize",
                                           "checkTxVerticesCapacity", "checkTxEdgesCapacity",
                                           "verticesInTxUpdated", "verticesInTxRemoved",
                                           "removingEdgeOwner", "prepareDeletions",
                                           "prepareDeletions", "prepareUpdates", "prepareAdditions",
                                           "checkVertexExistIfCustomizedId",
                                           "checkAggregateProperty", "checkAggregateProperty",
                                           "checkNonnullProperty", "queryEdgesFromBackend",
                                           "commitPartOfEdgeDeletions", "optimizeQueries",
                                           "checkVertexLabel", "checkId",
                                           "queryVerticesFromBackend", "joinTxVertices",
                                           "joinTxEdges", "lockForUpdateProperty", "optimizeQuery",
                                           "verifyVerticesConditionQuery",
                                           "verifyEdgesConditionQuery", "indexQuery",
                                           "joinTxRecords", "propertyUpdated", "parseEntry",
                                           "traverseByLabel", "reset", "queryVerticesByIds",
                                           "filterUnmatchedRecords", "skipOffsetOrStopLimit",
                                           "filterExpiredResultFromFromBackend", "queryEdgesByIds",
                                           "matchEdgeSortKeys", "rightResultFromIndexQuery");
        Reflection.registerFieldsToFilter(IndexableTransaction.class, "$assertionsDisabled");
        Reflection.registerMethodsToFilter(IndexableTransaction.class, "indexTransaction",
                                           "commit2Backend", "reset");
        Reflection.registerFieldsToFilter(AbstractTransaction.class, "LOG", "ownerThread",
                                          "autoCommit", "closed", "committing",
                                          "committing2Backend", "graph", "store", "mutation",
                                          "serializer", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(AbstractTransaction.class, "beforeWrite",
                                           "prepareCommit", "params", "mutation", "commit2Backend",
                                           "autoCommit", "beforeRead", "afterWrite", "afterRead",
                                           "commitMutation2Backend", "checkOwnerThread", "doAction",
                                           "store", "reset");
        Reflection.registerFieldsToFilter(HugeFactory.class, "LOG", "NAME_REGEX", "graphs");
        Reflection.registerMethodsToFilter(HugeFactory.class, "lambda$0");
        Reflection.registerFieldsToFilter(SchemaElement.class, "graph", "id", "name", "userdata",
                                          "status");
        Reflection.registerFieldsToFilter(HugeVertex.class, "EMPTY_SET", "id", "label", "edges",
                                          "$assertionsDisabled",
                                          "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy",
                                          "$SWITCH_TABLE$com$baidu$hugegraph$type$define$HugeKeys");
        Reflection.registerMethodsToFilter(HugeVertex.class,
                                           "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy",
                                           "newProperty", "newProperty", "tx",
                                           "$SWITCH_TABLE$com$baidu$hugegraph$type$define$HugeKeys",
                                           "checkIdLength", "onUpdateProperty",
                                           "ensureFilledProperties", "clone", "clone");
        Reflection.registerFieldsToFilter(HugeElement.class, "EMPTY_MAP", "MAX_PROPERTIES", "graph",
                                          "properties", "expiredTime", "removed", "fresh",
                                          "propLoaded", "defaultValueUpdated",
                                          "$assertionsDisabled",
                                          "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Cardinality");
        Reflection.registerMethodsToFilter(HugeElement.class, "removed", "addProperty",
                                           "newProperty", "tx", "onUpdateProperty",
                                           "ensureFilledProperties", "propLoaded",
                                           "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Cardinality",
                                           "getIdValue", "fresh", "updateToDefaultValueIfNone",
                                           "copyProperties");
        Reflection.registerFieldsToFilter(HugeEdge.class, "id", "label", "name", "sourceVertex",
                                          "targetVertex", "isOutEdge", "$assertionsDisabled",
                                          "$SWITCH_TABLE$com$baidu$hugegraph$type$define$HugeKeys",
                                          "$SWITCH_TABLE$org$apache$tinkerpop$gremlin$structure$Direction");
        Reflection.registerMethodsToFilter(HugeEdge.class, "checkAdjacentVertexExist",
                                           "newProperty", "newProperty", "tx",
                                           "$SWITCH_TABLE$com$baidu$hugegraph$type$define$HugeKeys",
                                           "onUpdateProperty", "ensureFilledProperties",
                                           "$SWITCH_TABLE$org$apache$tinkerpop$gremlin$structure$Direction",
                                           "clone", "clone");
        Reflection.registerFieldsToFilter(HugeProperty.class, "owner", "pkey", "value");
        Reflection.registerFieldsToFilter(HugeVariables.class, "LOG", "VARIABLES", "VARIABLE_KEY",
                                          "VARIABLE_TYPE", "BYTE_VALUE", "BOOLEAN_VALUE",
                                          "INTEGER_VALUE", "LONG_VALUE", "FLOAT_VALUE",
                                          "DOUBLE_VALUE", "STRING_VALUE", "LIST", "SET", "TYPES",
                                          "params", "graph");
        Reflection.registerMethodsToFilter(HugeVariables.class, "createPropertyKey",
                                           "queryAllVariableVertices", "queryVariableVertex",
                                           "createVariableVertex", "removeVariableVertex",
                                           "extractSingleObject", "setProperty");
        Reflection.registerFieldsToFilter(SchemaManager.class, "transaction", "graph");
        Reflection.registerMethodsToFilter(SchemaManager.class, "lambda$0", "lambda$1", "lambda$2",
                                           "lambda$3", "checkExists");
        Reflection.registerFieldsToFilter(PropertyKeyBuilder.class, "id", "name", "dataType",
                                          "cardinality", "aggregateType", "checkExist", "userdata",
                                          "$assertionsDisabled");
        Reflection.registerMethodsToFilter(PropertyKeyBuilder.class, "lambda$0", "checkStableVars",
                                           "checkAggregateType", "hasSameProperties");
        Reflection.registerFieldsToFilter(AbstractBuilder.class, "transaction", "graph");
        Reflection.registerMethodsToFilter(AbstractBuilder.class, "rebuildIndex", "graph",
                                           "checkSchemaName", "validOrGenerateId",
                                           "lockCheckAndCreateSchema", "propertyKeyOrNull",
                                           "checkSchemaIdIfRestoringMode", "vertexLabelOrNull",
                                           "edgeLabelOrNull", "indexLabelOrNull",
                                           "updateSchemaStatus");
        Reflection.registerFieldsToFilter(VertexLabelBuilder.class, "id", "name", "idStrategy",
                                          "properties", "primaryKeys", "nullableKeys", "ttl",
                                          "ttlStartTime", "enableLabelIndex", "userdata",
                                          "checkExist", "$assertionsDisabled",
                                          "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Action",
                                          "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy");
        Reflection.registerMethodsToFilter(VertexLabelBuilder.class, "lambda$0",
                                           "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy",
                                           "checkStableVars",
                                           "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Action",
                                           "checkProperties", "checkNullableKeys",
                                           "checkIdStrategy", "checkPrimaryKeys",
                                           "hasSameProperties", "checkTtl", "checkUserdata",
                                           "mapPkId2Name", "mapPkId2Name");
        Reflection.registerFieldsToFilter(EdgeLabelBuilder.class, "id", "name", "sourceLabel",
                                          "targetLabel", "frequency", "properties", "sortKeys",
                                          "nullableKeys", "ttl", "ttlStartTime", "enableLabelIndex",
                                          "userdata", "checkExist", "$assertionsDisabled",
                                          "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Action");
        Reflection.registerMethodsToFilter(EdgeLabelBuilder.class, "lambda$0", "checkStableVars",
                                           "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Action",
                                           "checkProperties", "checkNullableKeys", "checkSortKeys",
                                           "checkRelation", "hasSameProperties", "checkTtl",
                                           "checkUserdata", "mapPkId2Name", "mapPkId2Name");
        Reflection.registerFieldsToFilter(IndexLabelBuilder.class, "id", "name", "baseType",
                                          "baseValue", "indexType", "indexFields", "userdata",
                                          "checkExist", "rebuild", "$assertionsDisabled",
                                          "$SWITCH_TABLE$com$baidu$hugegraph$type$define$DataType",
                                          "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IndexType");
        Reflection.registerMethodsToFilter(IndexLabelBuilder.class, "lambda$0", "checkStableVars",
                                           "$SWITCH_TABLE$com$baidu$hugegraph$type$define$DataType",
                                           "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IndexType",
                                           "checkBaseType", "checkIndexType", "checkFields4Range",
                                           "loadElement", "checkFields", "checkRepeatIndex",
                                           "checkRepeatIndex", "checkRepeatIndex",
                                           "checkPrimaryKeyIndex", "checkRepeatRangeIndex",
                                           "checkRepeatSearchIndex", "checkRepeatSecondaryIndex",
                                           "checkRepeatShardIndex", "checkRepeatUniqueIndex",
                                           "removeSubIndex", "hasSubIndex", "allStringIndex",
                                           "oneNumericField", "hasSameProperties");
        Reflection.registerFieldsToFilter(TaskManager.class, "LOG", "SCHEDULE_PERIOD", "THREADS",
                                          "MANAGER", "schedulers", "taskExecutor", "taskDbExecutor",
                                          "serverInfoDbExecutor", "schedulerExecutor", "contexts",
                                          "$assertionsDisabled");
        Reflection.registerMethodsToFilter(TaskManager.class, "lambda$0", "resetContext",
                                           "closeTaskTx", "setContext", "instance",
                                           "closeSchedulerTx", "notifyNewTask",
                                           "scheduleOrExecuteJob", "scheduleOrExecuteJobForGraph");
        Reflection.registerFieldsToFilter(StandardTaskScheduler.class, "LOG", "graph",
                                          "serverManager", "taskExecutor", "taskDbExecutor",
                                          "eventListener", "tasks", "taskTx", "NO_LIMIT",
                                          "PAGE_SIZE", "QUERY_INTERVAL", "MAX_PENDING_TASKS",
                                          "$assertionsDisabled");
        Reflection.registerMethodsToFilter(StandardTaskScheduler.class, "lambda$0", "lambda$1",
                                           "lambda$2", "lambda$3", "lambda$4", "lambda$5",
                                           "lambda$6", "lambda$7", "tx", "listenChanges",
                                           "unlistenChanges", "submitTask", "queryTask",
                                           "queryTask", "queryTask", "call", "call", "remove",
                                           "sleep", "taskDone", "serverManager", "supportsPaging",
                                           "restore", "checkOnMasterNode", "waitUntilTaskCompleted",
                                           "scheduleTasks", "executeTasksOnWorker",
                                           "cancelTasksOnWorker");
        Reflection.registerFieldsToFilter(HugeTask.class, "LOG", "DECOMPRESS_RATIO", "scheduler",
                                          "callable", "type", "name", "id", "parent",
                                          "dependencies", "description", "context", "create",
                                          "server", "load", "status", "progress", "update",
                                          "retries", "input", "result", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(HugeTask.class, "property", "scheduler", "scheduler",
                                           "asArray", "checkPropertySize", "checkPropertySize",
                                           "checkDependenciesSuccess", "toOrderSet", "done",
                                           "callable", "setException", "set", "result", "status");
        Reflection.registerFieldsToFilter(TaskCallable.class, "LOG", "ERROR_COMMIT",
                                          "ERROR_MESSAGES", "task", "graph", "lastSaveTime",
                                          "saveInterval");
        Reflection.registerMethodsToFilter(TaskCallable.class, "graph", "closeTx", "cancelled",
                                           "done", "task", "save", "needSaveWithEx");
        Reflection.registerFieldsToFilter(TaskCallable.SysTaskCallable.class, "params");
        Reflection.registerMethodsToFilter(TaskCallable.SysTaskCallable.class, "params", "params");
        Reflection.registerFieldsToFilter(StandardAuthManager.class, "CACHE_EXPIRE", "graph",
                                          "eventListener", "usersCache", "users", "groups",
                                          "targets", "belong", "access", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(StandardAuthManager.class, "lambda$0", "listenChanges",
                                           "unlistenChanges", "invalidCache", "initSchemaIfNeeded",
                                           "rolePermission", "rolePermission", "rolePermission",
                                           "cache");
        Reflection.registerFieldsToFilter(SchemaDefine.class, "graph", "label");
        Reflection.registerMethodsToFilter(SchemaDefine.class, "schema", "createPropertyKey",
                                           "createPropertyKey", "createPropertyKey",
                                           "existEdgeLabel", "createRangeIndex", "unhideField",
                                           "hideField", "existVertexLabel", "initProperties");
        Reflection.registerFieldsToFilter(EntityManager.class, "graph", "label", "deser",
                                          "NO_LIMIT", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(EntityManager.class, "toList", "graph", "tx",
                                           "commitOrRollback", "unhideLabel", "queryById",
                                           "queryEntity", "constructVertex", "save", "query");
        Reflection.registerFieldsToFilter(RelationshipManager.class, "graph", "label", "deser",
                                          "NO_LIMIT", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(RelationshipManager.class, "lambda$0", "toList", "graph",
                                           "tx", "commitOrRollback", "unhideLabel", "queryById",
                                           "queryRelationship", "newVertex", "save");
        Reflection.registerFieldsToFilter(CacheManager.class, "LOG", "INSTANCE",
                                          "TIMER_TICK_PERIOD", "LOG_TICK_COST_TIME", "caches",
                                          "timer");
        Reflection.registerMethodsToFilter(CacheManager.class, "access$0", "scheduleTimer",
                                           "instance");
        Reflection.registerFieldsToFilter(org.apache.hugegraph.concurrent.LockManager.class,
                                          "INSTANCE", "lockGroupMap");
        Reflection.registerMethodsToFilter(org.apache.hugegraph.concurrent.LockManager.class,
                                           "instance");
        Reflection.registerFieldsToFilter(ServerReporter.class, "instance", "gauges", "counters",
                                          "histograms", "meters", "timers");
        Reflection.registerMethodsToFilter(ServerReporter.class, "instance", "instance");
        Reflection.registerFieldsToFilter(com.codahale.metrics.ScheduledReporter.class, "LOG",
                                          "FACTORY_ID", "registry", "executor",
                                          "shutdownExecutorOnStop", "disabledMetricAttributes",
                                          "scheduledFuture", "filter", "durationFactor",
                                          "durationUnit", "rateFactor", "rateUnit");
        Reflection.registerMethodsToFilter(com.codahale.metrics.ScheduledReporter.class,
                                           "convertDuration", "convertRate", "getRateUnit",
                                           "getDurationUnit", "isShutdownExecutorOnStop",
                                           "getDisabledMetricAttributes", "calculateRateUnit",
                                           "createDefaultExecutor", "lambda$start$0", "start");
        Reflection.registerFieldsToFilter(JsonSerializer.class, "LBUF_SIZE", "INSTANCE");
        Reflection.registerMethodsToFilter(JsonSerializer.class, "writeIterator", "instance");
        Reflection.registerFieldsToFilter(HugeVertexStepStrategy.class, "serialVersionUID",
                                          "INSTANCE");
        Reflection.registerMethodsToFilter(HugeVertexStepStrategy.class, "instance");
        Reflection.registerFieldsToFilter(HugeGraphStepStrategy.class, "serialVersionUID",
                                          "INSTANCE");
        Reflection.registerMethodsToFilter(HugeGraphStepStrategy.class, "instance");
        Reflection.registerFieldsToFilter(HugeCountStepStrategy.class, "serialVersionUID",
                                          "INSTANCE");
        Reflection.registerMethodsToFilter(HugeCountStepStrategy.class, "lambda$0", "instance");

        // Enable this line to generate registration statement
        //genRegisterPrivateActions();
    }

    public static void filterCriticalSystemClasses() {
        // TODO: merge them in HugeSecurityManager after 1.5.0
        Reflection.registerMethodsToFilter(Class.class, "forName", "newInstance");
        Reflection.registerMethodsToFilter(ClassLoader.class, "loadClass", "newInstance");
        Reflection.registerMethodsToFilter(Method.class, "invoke", "setAccessible");
        Reflection.registerMethodsToFilter(Field.class, "set", "setAccessible");
        Reflection.registerMethodsToFilter(java.lang.reflect.Constructor.class, "newInstance",
                                           "setAccessible");
        Reflection.registerMethodsToFilter(Runtime.class, "exec", "getRuntime");
        Reflection.registerMethodsToFilter(ProcessBuilder.class, "command", "start",
                                           "startPipeline");
        Reflection.registerMethodsToFilter(loadClass("java.lang.ProcessImpl"), "forkAndExec",
                                           "setAccessible", "start");

        optionalMethodsToFilter("jdk.internal.reflect.MethodAccessor", "invoke");
        optionalMethodsToFilter("jdk.internal.reflect.NativeMethodAccessorImpl", "invoke");
    }

    @SuppressWarnings("unused")
    private static void genRegisterPrivateActions() {
        registerPrivateActions(Thread.class);
        registerPrivateActions(ThreadLocal.class);
        registerPrivateActions(InheritableThreadLocal.class);

        registerPrivateActions(StandardAuthenticator.class);
        registerPrivateActions(HugeFactoryAuthProxy.class);
        registerPrivateActions(HugeAuthenticator.User.class);

        registerPrivateActions(HugeGraphAuthProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.AuthManagerProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.TaskSchedulerProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.GraphTraversalSourceProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.TraversalStrategiesProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.VariablesProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.Context.class);
        registerPrivateActions(HugeGraphAuthProxy.ContextThreadPoolExecutor.class);
        registerPrivateActions(HugeGraphAuthProxy.ContextTask.class);

        for (Class<?> clazz : StandardHugeGraph.PROTECT_CLASSES) {
            registerPrivateActions(clazz);
        }

        registerPrivateActions(HugeFactory.class);
        registerPrivateActions(AbstractTransaction.class);

        registerPrivateActions(SchemaElement.class);
        registerPrivateActions(HugeVertex.class);
        registerPrivateActions(HugeEdge.class);
        registerPrivateActions(HugeProperty.class);
        registerPrivateActions(HugeVariables.class);

        registerPrivateActions(SchemaManager.class);
        registerPrivateActions(PropertyKeyBuilder.class);
        registerPrivateActions(VertexLabelBuilder.class);
        registerPrivateActions(EdgeLabelBuilder.class);
        registerPrivateActions(IndexLabelBuilder.class);

        registerPrivateActions(TaskManager.class);
        registerPrivateActions(StandardTaskScheduler.class);
        registerPrivateActions(HugeTask.class);
        registerPrivateActions(TaskCallable.class);
        registerPrivateActions(SysTaskCallable.class);

        registerPrivateActions(StandardAuthManager.class);
        registerPrivateActions(SchemaDefine.class);
        registerPrivateActions(EntityManager.class);
        registerPrivateActions(RelationshipManager.class);

        // Don't shield them because need to access by auth RPC
        //registerPrivateActions(HugeUser.class);
        //registerPrivateActions(RolePermission.class);
        //registerPrivateActions(HugeResource.class);

        registerPrivateActions(CacheManager.class);
        registerPrivateActions(LockManager.class);
        registerPrivateActions(ServerReporter.class);
        registerPrivateActions(JsonSerializer.class);
        registerPrivateActions(HugeVertexStepStrategy.class);
        registerPrivateActions(HugeGraphStepStrategy.class);
        registerPrivateActions(HugeCountStepStrategy.class);
    }

    private static void registerPrivateActions(Class<?> clazz) {
        while (clazz != Object.class) {
            List<String> fields = new ArrayList<>();
            for (Field field : clazz.getDeclaredFields()) {
                if (!Modifier.isPublic(field.getModifiers())) {
                    fields.add(field.getName());
                }
            }
            List<String> methods = new ArrayList<>();
            for (Method method : clazz.getDeclaredMethods()) {
                if (!Modifier.isPublic(method.getModifiers()) ||
                    PROTECT_METHODS.contains(method.getName())) {
                    methods.add(method.getName());
                }
            }
            registerClass(clazz, fields, methods);
            clazz = clazz.getSuperclass();
        }
    }

    private static void registerClass(Class<?> clazz, List<String> fields, List<String> methods) {
        if (clazz.getName().startsWith("java") || fields.isEmpty() && methods.isEmpty()) {
            return;
        }

        final String[] array = new String[fields.size()];
        try {
            Reflection.registerFieldsToFilter(clazz, fields.toArray(array));
            Reflection.registerMethodsToFilter(clazz, methods.toArray(array));
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("Filter already registered: class")) {
                return;
            }
            throw e;
        }

        String code;
        code = String.format("Reflection.registerFieldsToFilter(%s.class, \"%s\");",
                             clazz.getCanonicalName(), String.join("\", \"", fields));
        if (!fields.isEmpty()) {
            // CHECKSTYLE:OFF
            System.out.println(code);
            // CHECKSTYLE:ON
        }

        code = String.format("Reflection.registerMethodsToFilter(%s.class, \"%s\");",
                             clazz.getCanonicalName(), String.join("\", \"", methods));
        if (!methods.isEmpty()) {
            // CHECKSTYLE:OFF
            System.out.println(code);
            // CHECKSTYLE:ON
        }
    }

    private static Class<?> loadClass(String clazz) {
        try {
            return Class.forName(clazz);
        } catch (ClassNotFoundException e) {
            throw new HugeException(e.getMessage(), e);
        }
    }

    public static void optionalMethodsToFilter(String className, String... methodNames) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            LOG.debug("Internal class {} not found in this JDK implementation, skipping filter " +
                      "registration", className, e);
        }
        if (clazz != null) {
            Reflection.registerMethodsToFilter(clazz, methodNames);
        }
    }
}
