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

package org.apache.hugegraph.type.define;

/**
 * The status of one graph on one server. It describes that single server
 * only, the status of the graph across the cluster is the aggregate of the
 * status reported by every server.
 */
public enum GraphStatus {

    /*
     * The server started to open the graph and has not finished yet, it
     * can't answer requests against the graph.
     */
    LOADING,

    /*
     * The server opened the graph and bound it to its gremlin server, so it
     * can answer requests against the graph. It says nothing about the other
     * servers, and nothing about the schema of the graph being initialized.
     */
    READY,

    /*
     * The server failed to open the graph, the cause is carried by the
     * status message.
     */
    FAILED
}
