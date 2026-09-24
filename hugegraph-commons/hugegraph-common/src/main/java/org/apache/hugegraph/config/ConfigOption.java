/*
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

package org.apache.hugegraph.config;

import com.google.common.base.Predicate;

public class ConfigOption<T> extends TypedOption<T, T> {
    private boolean urlNormalize = false;
    private String defaultScheme = null;

    public ConfigOption<T> withUrlNormalization(String scheme) {
        this.urlNormalize = true;
        this.defaultScheme = scheme;
        return this;
    }

    public boolean needsUrlNormalization() {
        return this.urlNormalize;
    }

    public String getDefaultScheme() {
        return this.defaultScheme;
    }

    public ConfigOption(String name, String desc, T value) {
        this(name, desc, null, value);
    }

    @SuppressWarnings("unchecked")
    public ConfigOption(String name, String desc, Predicate<T> pred, T value) {
        this(name, false, desc, pred, (Class<T>) value.getClass(), value);
    }

    public ConfigOption(String name, boolean required, String desc,
                        Predicate<T> pred, Class<T> type, T value) {
        super(name, required, desc, pred, type, value);
    }
}
