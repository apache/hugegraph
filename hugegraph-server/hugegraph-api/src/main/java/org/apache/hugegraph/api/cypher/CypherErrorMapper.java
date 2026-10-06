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

package org.apache.hugegraph.api.cypher;

/**
 * Classify cypher execution failures into stable error codes and attach
 * actionable hints for the most common, cryptic translator messages.
 */
public final class CypherErrorMapper {

    public static final String SYNTAX_ERROR = "HugeGraph.Cypher.SyntaxError";
    public static final String UNSUPPORTED_FEATURE =
            "HugeGraph.Cypher.UnsupportedFeature";
    public static final String MISSING_PARAMETER =
            "HugeGraph.Cypher.MissingParameter";
    public static final String EXECUTION_ERROR = "HugeGraph.Cypher.ExecutionError";

    private CypherErrorMapper() {
    }

    public static CypherModel.CypherError map(Throwable e) {
        String message = e.getMessage() != null ? e.getMessage() : e.toString();
        // strip noisy exception class prefixes, e.g. "...driver.exception.ResponseException: "
        message = message.replaceFirst(
                "^([a-zA-Z0-9_]+\\.)+[A-Za-z0-9_]+(Exception|Error):\\s*", "");
        String lower = message.toLowerCase();

        if (lower.contains("not defined") && lower.contains("$")) {
            return new CypherModel.CypherError(MISSING_PARAMETER, message,
                    "Provide the value via the 'parameters' query argument, " +
                    "e.g. ?parameters=%7B%22city%22%3A%20%22Beijing%22%7D");
        }
        if (lower.contains("undefined vertex label")
                || lower.contains("undefined edge label")
                || lower.contains("undefined property key")) {
            return new CypherModel.CypherError(EXECUTION_ERROR, message,
                    "Create the schema element via the schema API before " +
                    "running the query");
        }
        if (lower.contains("not defined") || lower.contains("undefined")) {
            return new CypherModel.CypherError(SYNTAX_ERROR, message,
                    "Declare the variable in a MATCH, UNWIND or WITH clause " +
                    "before referencing it");
        }
        if (lower.contains("not supported") || lower.contains("unsupported")) {
            return new CypherModel.CypherError(UNSUPPORTED_FEATURE, message,
                    "This construct is not covered by the built-in translator, " +
                    "see the Cypher compatibility guide for supported syntax");
        }
        return new CypherModel.CypherError(EXECUTION_ERROR, message, "");
    }
}
