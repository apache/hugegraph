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

package org.apache.hugegraph.auth;

import org.apache.commons.lang3.StringUtils;

public enum HugeDefaultRole {

    SPACE("space"),
    SPACE_MEMBER("space_member"),
    ANALYST("analyst"),
    OBSERVER("observer");

    public static final String DEFAULT_SPACE_TARGET_KEY = "DEFAULT_SPACE_TARGET";
    private final String name;

    HugeDefaultRole(String name) {
        this.name = name;
    }

    public static boolean isObserver(String role) {
        return (role.endsWith(OBSERVER.name) &&
                OBSERVER.name.length() != role.length());
    }

    public static String getNickname(String role) {
        if (isObserver(role)) {
            String graph = role.substring(0, role.lastIndexOf("_"));
            return graph + "-观察者";
        } else if (SPACE.name.equals(role)) {
            return "图空间管理员";
        } else if (SPACE_MEMBER.name.equals(role)) {
            return "图空间成员";
        } else if (ANALYST.name.equals(role)) {
            return "分析师";
        } else {
            return role;
        }
    }

    public static boolean isDefaultNickname(String nickname) {
        return StringUtils.isNotEmpty(nickname) &&
               ("图空间管理员".equals(nickname) ||
                "图空间成员".equals(nickname) ||
                "分析师".equals(nickname) ||
                nickname.endsWith("-观察者"));
    }

    public static boolean isDefault(String role) {
        return isObserver(role) || SPACE.name.equals(role) ||
               SPACE_MEMBER.name.equals(role) ||
               ANALYST.name.equals(role);
    }

    public static boolean isDefaultTarget(String target) {
        return target.endsWith(DEFAULT_SPACE_TARGET_KEY);
    }

    @Override
    public String toString() {
        return this.name;
    }

    public boolean isGraphRole() {
        return this.ordinal() >= OBSERVER.ordinal();
    }
}
