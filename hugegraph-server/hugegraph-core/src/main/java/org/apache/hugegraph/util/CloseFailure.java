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

package org.apache.hugegraph.util;

public final class CloseFailure {

    private CloseFailure() {
    }

    public static Throwable collect(Throwable first, Throwable next) {
        if (first == null || first == next) {
            return first == null ? next : first;
        }
        Throwable primary = next instanceof Error && !(first instanceof Error) ? next : first;
        Throwable secondary = primary == first ? next : first;
        primary.addSuppressed(secondary);
        return primary;
    }

}
