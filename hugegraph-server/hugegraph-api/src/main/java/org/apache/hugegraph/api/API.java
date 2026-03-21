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

package org.apache.hugegraph.api;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.SchemaTemplate;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.core.MediaType;

public class API {

    public static final String CHARSET = "UTF-8";
    public static final String TEXT_PLAIN = MediaType.TEXT_PLAIN;
    public static final String APPLICATION_JSON = MediaType.APPLICATION_JSON;
    public static final String APPLICATION_JSON_WITH_CHARSET =
            APPLICATION_JSON + ";charset=" + CHARSET;
    public static final String APPLICATION_TEXT_WITH_CHARSET =
            MediaType.TEXT_PLAIN + ";charset=" + CHARSET;
    public static final String JSON = MediaType.APPLICATION_JSON_TYPE
            .getSubtype();
    public static final String ACTION_APPEND = "append";
    public static final String ACTION_ELIMINATE = "eliminate";
    public static final String ACTION_UPDATE = "update";
    public static final String ACTION_CLEAR = "clear";
    public static final String USER_NAME_PATTERN = "^[0-9a-zA-Z_.-]{2,64}$";
    public static final String USER_PASSWORD_PATTERN = "[a-zA-Z0-9~!@#$%^&*()" +
                                                       "_+|<>,.?/:;" +
                                                       "'`\"\\[\\]{}\\\\]{5," +
                                                       "16}";
    public static final String USER_NICKNAME_PATTERN = "^(?!_)(?!.*?_$)" +
                                                       "[a-zA-Z0-9\u4e00-\u9fa5~!@#$" +
                                                       "%^&*()_+|<>,.?/:;" +
                                                       "'`\"\\[\\]{}\\\\]{1,16}$";
    protected static final Logger LOG = Log.logger(API.class);
    private static final Meter SUCCEED_METER =
            MetricsUtil.registerMeter(API.class, "commit-succeed");
    private static final Meter ILLEGAL_ARG_ERROR_METER =
            MetricsUtil.registerMeter(API.class, "illegal-arg");
    private static final Meter EXPECTED_ERROR_METER =
            MetricsUtil.registerMeter(API.class, "expected-error");
    private static final Meter UNKNOWN_ERROR_METER =
            MetricsUtil.registerMeter(API.class, "unknown-error");
    private static final Meter succeedMeter =
            MetricsUtil.registerMeter(API.class, "commit-succeed");
    private static final Meter illegalArgErrorMeter =
            MetricsUtil.registerMeter(API.class, "illegal-arg");
    private static final Meter expectedErrorMeter =
            MetricsUtil.registerMeter(API.class, "expected-error");
    private static final Meter unknownErrorMeter =
            MetricsUtil.registerMeter(API.class, "unknown-error");
    private static final String STANDALONE_ERROR =
            "GraphSpace management is not supported in standalone mode";

    public static HugeGraph graph(GraphManager manager, String graphSpace,
                                  String graph) {
        HugeGraph g = manager.graph(graphSpace, graph);
        if (g == null) {
            throw new NotFoundException(String.format(
                    "Graph '%s' does not exist", graph));
        }
        return g;
    }

    public static GraphSpace space(GraphManager manager, String space) {
        GraphSpace s = manager.graphSpace(space);
        if (s == null) {
            throw new NotFoundException(String.format(
                    "Graph space '%s' does not exist", space));
        }
        return s;
    }

    public static Service service(GraphManager manager, String graphSpace,
                                  String service) {
        Service s = manager.service(graphSpace, service);
        if (s == null) {
            throw new NotFoundException(String.format(
                    "Service '%s' does not exist", service));
        }
        return s;
    }

    public static SchemaTemplate schemaTemplate(GraphManager manager,
                                                String graphSpace,
                                                String schemaTemplate) {
        SchemaTemplate st = manager.schemaTemplate(graphSpace, schemaTemplate);
        if (st == null) {
            throw new NotFoundException(String.format(
                    "Schema template '%s' does not exist", schemaTemplate));
        }
        return st;
    }

    public static HugeGraph graph4admin(GraphManager manager, String graphSpace,
                                        String graph) {
        return graph(manager, graphSpace, graph).hugegraph();
    }

    public static <R> R commit(HugeGraph g, Callable<R> callable) {
        Consumer<Throwable> rollback = (error) -> {
            if (error != null) {
                LOG.error("Failed to commit", error);
            }
            try {
                g.tx().rollback();
            } catch (Throwable e) {
                LOG.error("Failed to rollback", e);
            }
        };

        try {
            R result = callable.call();
            g.tx().commit();
            SUCCEED_METER.mark();
            return result;
        } catch (IllegalArgumentException | NotFoundException |
                 ForbiddenException e) {
            ILLEGAL_ARG_ERROR_METER.mark();
            rollback.accept(null);
            throw e;
        } catch (RuntimeException e) {
            EXPECTED_ERROR_METER.mark();
            rollback.accept(e);
            throw e;
        } catch (Throwable e) {
            UNKNOWN_ERROR_METER.mark();
            rollback.accept(e);
            // TODO: throw the origin exception 'e'
            throw new HugeException("Failed to commit", e);
        }
    }

    public static void commit(HugeGraph g, Runnable runnable) {
        commit(g, () -> {
            runnable.run();
            return null;
        });
    }

    public static Object[] properties(Map<String, Object> properties) {
        Object[] list = new Object[properties.size() * 2];
        int i = 0;
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
            list[i++] = prop.getKey();
            list[i++] = prop.getValue();
        }
        return list;
    }

    protected static void checkCreatingBody(Checkable body) {
        E.checkArgumentNotNull(body, "The request body can't be empty");
        body.checkCreate(false);
    }

    protected static void checkUpdatingBody(Checkable body) {
        E.checkArgumentNotNull(body, "The request body can't be empty");
        body.checkUpdate();
    }

    protected static void checkCreatingBody(Collection<? extends Checkable> bodies) {
        E.checkArgumentNotNull(bodies, "The request body can't be empty");
        for (Checkable body : bodies) {
            E.checkArgument(body != null,
                            "The batch body can't contain null record");
            body.checkCreate(true);
        }
    }

    protected static void checkUpdatingBody(Collection<? extends Checkable> bodies) {
        E.checkArgumentNotNull(bodies, "The request body can't be empty");
        for (Checkable body : bodies) {
            E.checkArgumentNotNull(body,
                                   "The batch body can't contain null record");
            body.checkUpdate();
        }
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> parseProperties(String properties) {
        if (properties == null || properties.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<String, Object> props = null;
        try {
            props = JsonUtil.fromJson(properties, Map.class);
        } catch (Exception ignored) {
            // ignore
        }

        // If properties is the string "null", props will be null
        E.checkArgument(props != null,
                        "Invalid request with properties: %s", properties);
        return props;
    }

    public static boolean checkAndParseAction(String action) {
        E.checkArgumentNotNull(action, "The action param can't be empty");
        if (action.equals(ACTION_APPEND)) {
            return true;
        } else if (action.equals(ACTION_ELIMINATE)) {
            return false;
        } else {
            throw new NotSupportedException(String.format("Not support action '%s'", action));
        }
    }

    /**
     * Ensures the graph manager is available and PD mode is enabled.
     *
     * @param manager the graph manager of current request
     * @throws IllegalArgumentException if the graph manager is null
     * @throws HugeException if PD mode is disabled
     */
    protected static void ensurePdModeEnabled(GraphManager manager) {
        E.checkArgumentNotNull(manager, "Graph manager can't be null");
        if (!manager.isPDEnabled()) {
            throw new HugeException(STANDALONE_ERROR);
        }
    }

    public static boolean hasAdminPerm(GraphManager manager, String user) {
        return manager.authManager().isAdminManager(user);
    }

    public static boolean hasSpaceManagerPerm(GraphManager manager,
                                              String graphSpace,
                                              String user) {
        return manager.authManager().isSpaceManager(graphSpace, user);
    }

    public static boolean hasAdminOrSpaceManagerPerm(GraphManager manager,
                                                     String graphSpace,
                                                     String user) {
        return hasAdminPerm(manager, user) ||
               hasSpaceManagerPerm(manager, graphSpace, user);
    }

    public static void validPermission(boolean hasPermission, String user,
                                       String action) {
        if (!hasPermission) {
            throw new jakarta.ws.rs.ForbiddenException(
                    String.format("The user [%s] has no permission to [%s].", user, action));
        }
    }

    public static class ApiMeasurer {

        public static final String EDGE_ITER = "edge_iterations";
        public static final String VERTICE_ITER = "vertice_iterations";
        public static final String COST = "cost(ns)";
        private final long timeStart;
        private final Map<String, Object> measures;

        public ApiMeasurer() {
            this.timeStart = System.nanoTime();
            this.measures = InsertionOrderUtil.newMap();
        }

        public Map<String, Object> measures() {
            measures.put(COST, System.nanoTime() - timeStart);
            return measures;
        }

        public void put(String key, String value) {
            this.measures.put(key, value);
        }

        public void put(String key, long value) {
            this.measures.put(key, value);
        }

        public void put(String key, int value) {
            this.measures.put(key, value);
        }

        protected void addCount(String key, long value) {
            Object current = measures.get(key);
            if (current == null) {
                measures.put(key, new MutableLong(value));
            } else if (current instanceof MutableLong) {
                ((MutableLong) measures.computeIfAbsent(key, MutableLong::new)).add(value);
            } else if (current instanceof Long) {
                Long currentLong = (Long) current;
                measures.put(key, new MutableLong(currentLong + value));
            } else {
                throw new NotSupportedException("addCount() method's 'value' datatype must be " +
                                                "Long or MutableLong");
            }
        }

        public void addIterCount(long verticeIters, long edgeIters) {
            this.addCount(EDGE_ITER, edgeIters);
            this.addCount(VERTICE_ITER, verticeIters);
        }
    }
}
