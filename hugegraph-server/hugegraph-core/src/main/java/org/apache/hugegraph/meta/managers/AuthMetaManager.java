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

package org.apache.hugegraph.meta.managers;

import static org.apache.hugegraph.meta.MetaManager.META_PATH_ACCESS;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_AUTH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_AUTH_EVENT;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_BELONG;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_DELIMITER;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPHSPACE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GROUP;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_HUGEGRAPH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_PROJECT;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_ROLE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_TARGET;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_USER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.auth.HugeAccess;
import org.apache.hugegraph.auth.HugeBelong;
import org.apache.hugegraph.auth.HugeGroup;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeProject;
import org.apache.hugegraph.auth.HugeRole;
import org.apache.hugegraph.auth.HugeTarget;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;

public class AuthMetaManager extends AbstractMetaManager {

    public AuthMetaManager(MetaDriver metaDriver, String cluster) {
        super(metaDriver, cluster);
    }

    public void createUser(HugeUser user) throws IOException {
        String result = this.metaDriver.get(userKey(user.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The user name '%s' has existed", user.name());
        this.metaDriver.put(userKey(user.name()), serialize(user));
    }

    public HugeUser updateUser(HugeUser user) throws IOException {
        String result = this.metaDriver.get(userKey(user.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The user name '%s' does not existed", user.name());

        HugeUser ori = HugeUser.fromMap(JsonUtil.fromJson(result, Map.class));
        ori.update(new Date());
        ori.nickname(user.nickname());
        ori.password(user.password());
        ori.phone(user.phone());
        ori.email(user.email());
        ori.avatar(user.avatar());
        ori.description(user.description());
        this.metaDriver.put(userKey(user.name()), serialize(ori));
        return ori;
    }

    public HugeUser deleteUser(Id id) throws IOException,
                                             ClassNotFoundException {
        String result = this.metaDriver.get(userKey(id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The user name '%s' does not existed", id.asString());
        this.metaDriver.delete(userKey(id.asString()));
        this.putAuthEvent(new MetaManager.AuthEvent("DELETE", "USER", id.asString()));
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeUser.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeUser findUser(String name)
            throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(userKey(name));
        if (StringUtils.isEmpty(result)) {
            return null;
        }

        return HugeUser.fromMap(JsonUtil.fromJson(result, Map.class));
    }

    public List<HugeUser> listUsers(List<Id> ids) throws IOException,
                                                         ClassNotFoundException {
        List<HugeUser> result = new ArrayList<>();
        Map<String, String> userMap =
                this.metaDriver.scanWithPrefix(userListKey());
        for (Id id : ids) {
            if (userMap.containsKey(userKey(id.asString()))) {
                String value = userMap.get(userKey(id.asString()));
                Map<String, Object> map = JsonUtil.fromJson(value, Map.class);
                HugeUser user = HugeUser.fromMap(map);
                result.add(user);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeUser> listUsersByGroup(String group, long limit)
            throws IOException, ClassNotFoundException {
        List<HugeUser> result = new ArrayList<>();
        Map<String, String> userMap =
                this.metaDriver.scanWithPrefix(userListKey());
        for (Map.Entry<String, String> item : userMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeUser user = HugeUser.fromMap(map);
            result.add(user);
        }

        List<HugeBelong> belongs = new ArrayList<>();
        Map<String, String> belongMap = this.metaDriver.scanWithPrefix(
                belongListKey("*"));
        for (Map.Entry<String, String> item : belongMap.entrySet()) {
            if (limit >= 0 && belongs.size() >= limit) {
                break;
            }
            String groupName = arrayFromBelong(item.getKey())[2];
            if (groupName.equals(group)) {
                Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                            Map.class);
                HugeBelong belong = HugeBelong.fromMap(map);
                belongs.add(belong);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeUser> listAllUsers(long limit)
            throws IOException,
                   ClassNotFoundException {
        List<HugeUser> result = new ArrayList<>();
        Map<String, String> userMap =
                this.metaDriver.scanWithPrefix(userListKey());
        for (Map.Entry<String, String> item : userMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeUser user = HugeUser.fromMap(map);
            result.add(user);
        }

        return result;
    }

    public Id createGroup(HugeGroup group) throws IOException {
        String key = groupKey(group.name());
        String result = this.metaDriver.get(key);
        E.checkArgument(StringUtils.isEmpty(result),
                        "The group name '%s' has existed", group.name());
        this.metaDriver.put(key, serialize(group));
        return group.id();
    }

    public HugeGroup updateGroup(HugeGroup group) throws IOException {
        String key = groupKey(group.name());
        String result = this.metaDriver.get(key);
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The group name '%s' is not existed", group.name());
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeGroup ori = HugeGroup.fromMap(map);
        ori.update(new Date());
        ori.nickname(group.nickname());
        ori.description(group.description());
        this.metaDriver.put(key, serialize(ori));
        return ori;
    }

    public HugeGroup deleteGroup(Id id) throws IOException,
                                               ClassNotFoundException {
        String name = id.asString();
        String key = groupKey(name);
        String result = this.metaDriver.get(key);
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The group name '%s' is not existed", name);
        this.metaDriver.delete(key);
        this.putAuthEvent(new MetaManager.AuthEvent("DELETE", "GROUP",
                                                    name));
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeGroup.fromMap(map);
    }

    public HugeGroup findGroup(String name) {
        String result = this.metaDriver.get(groupKey(name));
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        return HugeGroup.fromMap(JsonUtil.fromJson(result, Map.class));
    }

    public List<HugeGroup> listGroups(long limit) throws IOException,
                                                         ClassNotFoundException {
        List<HugeGroup> result = new ArrayList<>();
        Map<String, String> groupMap =
                this.metaDriver.scanWithPrefix(groupListKey());
        for (Map.Entry<String, String> item : groupMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeGroup group = HugeGroup.fromMap(map);
            result.add(group);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public Id createRole(String graphSpace, HugeRole role)
            throws IOException {
        Id roleId = IdGenerator.of(role.name());
        HugeRole existed = this.findRole(graphSpace, roleId);
        // not support too many role to share same id
        E.checkArgument(existed == null, "The role name '%s' has existed",
                        role.name());
        role.name(roleId.asString());

        this.metaDriver.put(roleKey(graphSpace, role.name()),
                            serialize(role));
        return roleId;
    }

    @SuppressWarnings("unchecked")
    public HugeRole updateRole(String graphSpace, HugeRole role)
            throws IOException {
        String result = this.metaDriver.get(roleKey(graphSpace, role.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The role name '%s' is not existed", role.name());

        // only description and update-time could be updated
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeRole ori = HugeRole.fromMap(map);
        ori.update(new Date());
        ori.nickname(role.nickname());
        ori.description(role.description());
        this.metaDriver.put(roleKey(graphSpace, ori.name()),
                            serialize(ori));
        return ori;
    }

    @SuppressWarnings("unchecked")
    public HugeRole deleteRole(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        String result = this.metaDriver.get(roleKey(graphSpace,
                                                    id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The role name '%s' is not existed", id.asString());
        this.metaDriver.delete(roleKey(graphSpace, id.asString()));
        this.putAuthEvent(new MetaManager.AuthEvent("DELETE", "ROLE", id.asString()));
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeRole.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeRole findRole(String graphSpace, Id id) {
        String result = this.metaDriver.get(roleKey(graphSpace,
                                                    id.asString()));
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeRole.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeRole getRole(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        String result = this.metaDriver.get(roleKey(graphSpace,
                                                    id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The role name '%s' is not existed", id.asString());
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeRole.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public List<HugeRole> listRoles(String graphSpace, List<Id> ids)
            throws IOException,
                   ClassNotFoundException {
        List<HugeRole> result = new ArrayList<>();
        Map<String, String> roleMap =
                this.metaDriver.scanWithPrefix(roleListKey(graphSpace));
        for (Id id : ids) {
            if (roleMap.containsKey(roleKey(graphSpace, id.asString()))) {
                String roleString = roleMap.get(roleKey(graphSpace,
                                                        id.asString()));
                Map<String, Object> map = JsonUtil.fromJson(roleString,
                                                            Map.class);
                HugeRole role = HugeRole.fromMap(map);
                result.add(role);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeRole> listAllRoles(String graphSpace, long limit)
            throws IOException,
                   ClassNotFoundException {
        List<HugeRole> result = new ArrayList<>();
        Map<String, String> roleMap =
                this.metaDriver.scanWithPrefix(roleListKey(graphSpace));
        for (Map.Entry<String, String> item : roleMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeRole role = HugeRole.fromMap(map);
            result.add(role);
        }

        return result;
    }

    public Id createTarget(String graphSpace, HugeTarget target)
            throws IOException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      target.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The target name '%s' has existed", target.name());
        this.metaDriver.put(targetKey(graphSpace, target.name()),
                            serialize(target));
        return target.id();
    }

    @SuppressWarnings("unchecked")
    public HugeTarget updateTarget(String graphSpace, HugeTarget target)
            throws IOException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      target.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", target.name());

        // only url, graph, description, resources and update-time could be updated
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeTarget ori = deserializeTarget(graphSpace, map);
        ori.update(new Date());
        ori.url(target.url());
        ori.graph(target.graph());
        ori.description(target.description());
        ori.resources(target.resources());
        this.metaDriver.put(targetKey(graphSpace, target.name()),
                            serialize(ori));
        this.putAuthEvent(new MetaManager.AuthEvent("UPDATE", "TARGET",
                                                    ori.id().asString()));
        return ori;
    }

    @SuppressWarnings("unchecked")
    public HugeTarget deleteTarget(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", id.asString());
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeTarget target = deserializeTarget(graphSpace, map);
        this.metaDriver.delete(targetKey(graphSpace, id.asString()));
        this.putAuthEvent(new MetaManager.AuthEvent("DELETE", "TARGET",
                                                    id.asString()));
        return target;
    }

    @SuppressWarnings("unchecked")
    public HugeTarget findTarget(String graphSpace, Id id) {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      id.asString()));
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return deserializeTarget(graphSpace, map);
    }

    @SuppressWarnings("unchecked")
    public HugeTarget getTarget(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", id.asString());
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return deserializeTarget(graphSpace, map);
    }

    @SuppressWarnings("unchecked")
    public List<HugeTarget> listTargets(String graphSpace, List<Id> ids)
            throws IOException,
                   ClassNotFoundException {
        List<HugeTarget> result = new ArrayList<>();
        Map<String, String> targetMap =
                this.metaDriver.scanWithPrefix(targetListKey(graphSpace));
        for (Id id : ids) {
            if (targetMap.containsKey(targetKey(graphSpace, id.asString()))) {
                String targetString = targetMap.get(targetKey(graphSpace,
                                                              id.asString()));
                Map<String, Object> map = JsonUtil.fromJson(targetString,
                                                            Map.class);
                HugeTarget target = deserializeTarget(graphSpace, map);
                result.add(target);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeTarget> listAllTargets(String graphSpace, long limit)
            throws IOException,
                   ClassNotFoundException {
        List<HugeTarget> result = new ArrayList<>();
        Map<String, String> targetMap =
                this.metaDriver.scanWithPrefix(targetListKey(graphSpace));
        for (Map.Entry<String, String> item : targetMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeTarget target = deserializeTarget(graphSpace, map);
            result.add(target);
        }

        return result;
    }

    private static HugeTarget deserializeTarget(String graphSpace,
                                                 Map<String, Object> map) {
        String key = Hidden.unHide(HugeTarget.P.GRAPHSPACE);
        Map<String, Object> scoped = new HashMap<>(map);
        if (!scoped.containsKey(key)) {
            scoped.put(key, graphSpace);
        } else {
            E.checkArgument(graphSpace.equals(scoped.get(key)),
                            "The target graphspace '%s' does not match '%s'",
                            scoped.get(key), graphSpace);
        }
        return HugeTarget.fromMap(scoped);
    }

    public Id createBelong(String graphSpace, HugeBelong belong)
            throws IOException, ClassNotFoundException {
        String belongId = this.checkBelong(graphSpace, belong);
        String result = this.metaDriver.get(belongKey(graphSpace, belongId));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The belong name '%s' has existed", belongId);
        this.metaDriver.put(belongKey(graphSpace, belongId), serialize(belong));
        this.putAuthEvent(new MetaManager.AuthEvent("CREATE", "BELONG", belongId));
        return IdGenerator.of(belongId);
    }

    @SuppressWarnings("unchecked")
    public HugeBelong updateBelong(String graphSpace, HugeBelong belong)
            throws IOException, ClassNotFoundException {
        String belongId = this.checkBelong(graphSpace, belong);
        String result = this.metaDriver.get(belongKey(graphSpace, belongId));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The belong name '%s' is not existed", belongId);

        // only description and update-time could be updated
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeBelong ori = HugeBelong.fromMap(map);
        ori.update(new Date());
        ori.description(belong.description());
        this.metaDriver.put(belongKey(graphSpace, belongId), serialize(ori));
        return ori;
    }

    public String checkBelong(String graphSpace, HugeBelong belong)
            throws IOException, ClassNotFoundException {
        String source = belong.source().asString();
        String target = belong.target().asString();
        String link = belong.link();
        HugeUser user = this.findUser(source);
        HugeGroup group = this.findGroup(source);
        E.checkArgument(user != null || group != null,
                        "The source name '%s' is not existed",
                        source);
        HugeGroup groupTarget = this.findGroup(target);
        HugeRole role = this.findRole(graphSpace, belong.target());
        E.checkArgument(role != null || groupTarget != null,
                        "The target name '%s' is not existed",
                        target);

        return belongId(source, target, link);
    }

    @SuppressWarnings("unchecked")
    public HugeBelong deleteBelong(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        String result = this.metaDriver.get(belongKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The belong name '%s' is not existed", id.asString());
        this.metaDriver.delete(belongKey(graphSpace, id.asString()));
        this.putAuthEvent(new MetaManager.AuthEvent("DELETE", "BELONG", id.asString()));
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeBelong.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeBelong getBelong(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        String result = this.metaDriver.get(belongKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The belong name '%s' is not existed", id.asString());

        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeBelong.fromMap(map);
    }

    public boolean existBelong(String graphSpace, Id id) {
        String result = this.metaDriver.get(belongKey(graphSpace,
                                                      id.asString()));
        return StringUtils.isNotEmpty(result);
    }

    @SuppressWarnings("unchecked")
    public List<HugeBelong> listBelong(String graphSpace, List<Id> ids)
            throws IOException,
                   ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap =
                this.metaDriver.scanWithPrefix(belongListKey(graphSpace));
        for (Id id : ids) {
            if (belongMap.containsKey(belongKey(graphSpace, id.asString()))) {
                String belongString = belongMap.get(belongKey(graphSpace,
                                                              id.asString()));
                Map<String, Object> map = JsonUtil.fromJson(belongString,
                                                            Map.class);
                HugeBelong belong = HugeBelong.fromMap(map);
                result.add(belong);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeBelong> listAllBelong(String graphSpace, long limit)
            throws IOException,
                   ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap =
                this.metaDriver.scanWithPrefix(belongListKey(graphSpace));
        for (Map.Entry<String, String> item : belongMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeBelong belong = HugeBelong.fromMap(map);
            result.add(belong);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeBelong> listBelongBySource(String graphSpace, Id source,
                                               String link, long limit)
            throws IOException,
                   ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();

        String sourceLink = (HugeBelong.ALL.equals(link)) ? source.asString() :
                            source.asString() + "->" + link;

        String key = belongListKeyBySource(graphSpace, sourceLink);

        Map<String, String> belongMap = this.metaDriver.scanWithPrefix(key);
        for (Map.Entry<String, String> item : belongMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeBelong belong = HugeBelong.fromMap(map);
            result.add(belong);
        }

        return result;
    }

    public String[] arrayFromBelong(String belongKey) {
        E.checkArgument(StringUtils.isNotEmpty(belongKey),
                        "The belong name '%s' is empty", belongKey);
        E.checkArgument(belongKey.contains("->"),
                        "The belong name '%s' is invalid", belongKey);
        String[] items = belongKey.split("->");
        E.checkArgument(items.length == 3,
                        "The belong name '%s' is invalid", belongKey);
        return items;
    }

    @SuppressWarnings("unchecked")
    public List<HugeBelong> listBelongByTarget(String graphSpace,
                                               Id role, String link, long limit)
            throws IOException,
                   ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap = this.metaDriver.scanWithPrefix(
                belongListKey(graphSpace));
        for (Map.Entry<String, String> item : belongMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            String[] array = arrayFromBelong(item.getKey());
            String linkName = array[1];
            String roleName = array[2];
            if ((linkName.equals(link) || "*".equals(link)) &&
                roleName.equals(role.asString())) {
                Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                            Map.class);
                HugeBelong belong = HugeBelong.fromMap(map);
                result.add(belong);
            }
        }

        return result;
    }

    public Id createAccess(String graphSpace, HugeAccess access)
            throws IOException, ClassNotFoundException {
        String accessId = this.checkAccess(graphSpace, access);
        String result = this.metaDriver.get(accessKey(graphSpace, accessId));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The access name '%s' has existed", accessId);
        this.metaDriver.put(accessKey(graphSpace, accessId), serialize(access));
        this.putAuthEvent(new MetaManager.AuthEvent("CREATE", "ACCESS", accessId));
        return IdGenerator.of(accessId);
    }

    @SuppressWarnings("unchecked")
    public HugeAccess updateAccess(String graphSpace, HugeAccess access)
            throws IOException, ClassNotFoundException {
        String accessId = this.checkAccess(graphSpace, access);
        String result = this.metaDriver.get(accessKey(graphSpace, accessId));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", accessId);
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeAccess existed = HugeAccess.fromMap(map);
        E.checkArgument(existed.permission().code() ==
                        access.permission().code(),
                        "The access name '%s' has existed", accessId);

        // only description and update-time could be updated
        Map<String, Object> oriMap = JsonUtil.fromJson(result, Map.class);
        HugeAccess ori = HugeAccess.fromMap(oriMap);
        ori.update(new Date());
        ori.description(access.description());
        this.metaDriver.put(accessKey(graphSpace, accessId), serialize(ori));
        return ori;
    }

    public String checkAccess(String graphSpace, HugeAccess access)
            throws IOException, ClassNotFoundException {
        // Try to find as role first, then as group
        String sourceName = null;
        HugeRole role = this.findRole(graphSpace, access.source());
        if (role != null) {
            sourceName = role.name();
        } else {
            // If not found as role, try to find as group
            HugeGroup group = this.findGroup(access.source().asString());
            if (group != null) {
                sourceName = group.name();
            }
        }

        E.checkArgument(sourceName != null,
                        "The role or group name '%s' is not existed",
                        access.source().asString());

        HugeTarget target = this.getTarget(graphSpace, access.target());
        E.checkArgument(target != null,
                        "The target name '%s' is not existed",
                        access.target().asString());

        return accessId(sourceName, target.name(), access.permission());
    }

    @SuppressWarnings("unchecked")
    public HugeAccess deleteAccess(String graphSpace, Id id)
            throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", id.asString());
        this.metaDriver.delete(accessKey(graphSpace, id.asString()));
        this.putAuthEvent(new MetaManager.AuthEvent("DELETE", "ACCESS", id.asString()));
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeAccess.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeAccess findAccess(String graphSpace, Id id) {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeAccess.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeAccess getAccess(String graphSpace, Id id)
            throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", id.asString());
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeAccess.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public List<HugeAccess> listAccess(String graphSpace, List<Id> ids)
            throws IOException,
                   ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap =
                this.metaDriver.scanWithPrefix(accessListKey(graphSpace));
        for (Id id : ids) {
            if (accessMap.containsKey(accessKey(graphSpace, id.asString()))) {
                String accessString = accessMap.get(accessKey(graphSpace,
                                                              id.asString()));
                Map<String, Object> map = JsonUtil.fromJson(accessString,
                                                            Map.class);
                HugeAccess access = HugeAccess.fromMap(map);
                result.add(access);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeAccess> listAllAccess(String graphSpace, long limit)
            throws IOException,
                   ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap =
                this.metaDriver.scanWithPrefix(accessListKey(graphSpace));
        for (Map.Entry<String, String> item : accessMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeAccess access = HugeAccess.fromMap(map);
            result.add(access);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeAccess> listAccessByRole(String graphSpace,
                                             Id role, long limit)
            throws IOException,
                   ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap = this.metaDriver.scanWithPrefix(
                accessListKeyByRole(graphSpace, role.asString()));
        for (Map.Entry<String, String> item : accessMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeAccess access = HugeAccess.fromMap(map);
            result.add(access);
        }

        return result;
    }

    public List<HugeAccess> listAccessByGroup(String graphSpace,
                                              Id group, long limit) {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap = this.metaDriver.scanWithPrefix(
                accessListKeyByGroup(graphSpace, group.asString()));
        for (Map.Entry<String, String> item : accessMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeAccess access = HugeAccess.fromMap(map);
            result.add(access);
        }

        return result;
    }

    public String targetFromAccess(String accessKey) {
        E.checkArgument(StringUtils.isNotEmpty(accessKey),
                        "The access name '%s' is empty", accessKey);
        E.checkArgument(accessKey.contains("->"),
                        "The access name '%s' is invalid", accessKey);
        String[] items = accessKey.split("->");
        E.checkArgument(items.length == 3,
                        "The access name '%s' is invalid", accessKey);
        return items[2];
    }

    public void clearGraphAuth(String graphSpace) {
        E.checkArgument(StringUtils.isNotEmpty(graphSpace),
                        "The graphSpace is empty");
        String prefix = this.authPrefix(graphSpace);
        this.metaDriver.deleteWithPrefix(prefix);
    }

    @SuppressWarnings("unchecked")
    public List<HugeAccess> listAccessByTarget(String graphSpace,
                                               Id target, long limit)
            throws IOException,
                   ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap = this.metaDriver.scanWithPrefix(
                accessListKey(graphSpace));
        for (Map.Entry<String, String> item : accessMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            String targetName = targetFromAccess(item.getKey());
            if (targetName.equals(target.asString())) {
                Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                            Map.class);
                HugeAccess access = HugeAccess.fromMap(map);
                result.add(access);
            }
        }

        return result;
    }

    public <T> void listenAuthEvent(Consumer<T> consumer) {
        this.listen(this.authEventKey(), consumer);
    }

    public void putAuthEvent(MetaManager.AuthEvent event) {
        this.metaDriver.put(authEventKey(), JsonUtil.toJson(event.asMap()));
    }

    public String belongId(String source, String target, String link) {
        E.checkArgument(StringUtils.isNotEmpty(source) &&
                        StringUtils.isNotEmpty(target),
                        "The source name '%s' or target name '%s' is empty",
                        source, target);
        return String.join("->", source, link, target);
    }

    public String accessId(String roleName, String targetName,
                           HugePermission permission) {
        E.checkArgument(StringUtils.isNotEmpty(roleName) &&
                        StringUtils.isNotEmpty(targetName),
                        "The role name '%s' or target name '%s' is empty",
                        roleName, targetName);
        String code = String.valueOf(permission.code());
        return String.join("->", roleName, code, targetName);
    }

    public String authEventKey() {
        // HUGEGRAPH/{cluster}/AUTH_EVENT
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_AUTH_EVENT);
    }

    public Id createProject(String graphSpace, HugeProject project)
            throws IOException {
        String result = this.metaDriver.get(projectKey(graphSpace,
                                                       project.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The project name '%s' has existed in graphSpace '%s'",
                        project.name(), graphSpace);
        this.metaDriver.put(projectKey(graphSpace, project.name()),
                            serialize(project));
        this.putAuthEvent(new MetaManager.AuthEvent("CREATE", "PROJECT",
                                                    project.id().asString()));
        return project.id();
    }

    @SuppressWarnings("unchecked")
    public HugeProject updateProject(String graphSpace, HugeProject project)
            throws IOException {
        String result = this.metaDriver.get(projectKey(graphSpace,
                                                       project.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The project name '%s' does not exist in graphSpace '%s'",
                        project.name(), graphSpace);

        // Update project
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeProject ori = HugeProject.fromMap(map);
        ori.update(new Date());
        ori.description(project.description());
        ori.graphs(project.graphs());
        ori.adminGroupId(project.adminGroupId());
        ori.opGroupId(project.opGroupId());
        ori.targetId(project.targetId());

        this.metaDriver.put(projectKey(graphSpace, project.name()),
                            serialize(ori));
        this.putAuthEvent(new MetaManager.AuthEvent("UPDATE", "PROJECT",
                                                    ori.id().asString()));
        return ori;
    }

    @SuppressWarnings("unchecked")
    public HugeProject deleteProject(String graphSpace, Id id)
            throws IOException, ClassNotFoundException {
        // Find project by id first
        Map<String, String> projectMap =
                this.metaDriver.scanWithPrefix(projectListKey(graphSpace));
        HugeProject project = null;
        String projectKey = null;

        for (Map.Entry<String, String> entry : projectMap.entrySet()) {
            Map<String, Object> map = JsonUtil.fromJson(entry.getValue(), Map.class);
            HugeProject p = HugeProject.fromMap(map);
            if (p.id().equals(id)) {
                project = p;
                projectKey = entry.getKey();
                break;
            }
        }

        E.checkArgument(project != null,
                        "The project with id '%s' does not exist in graphSpace '%s'",
                        id, graphSpace);

        this.metaDriver.delete(projectKey);
        this.putAuthEvent(new MetaManager.AuthEvent("DELETE", "PROJECT", id.asString()));
        return project;
    }

    @SuppressWarnings("unchecked")
    public HugeProject getProject(String graphSpace, Id id)
            throws IOException, ClassNotFoundException {
        // Find project by id
        Map<String, String> projectMap =
                this.metaDriver.scanWithPrefix(projectListKey(graphSpace));

        for (Map.Entry<String, String> entry : projectMap.entrySet()) {
            Map<String, Object> map = JsonUtil.fromJson(entry.getValue(), Map.class);
            HugeProject project = HugeProject.fromMap(map);
            if (project.id().equals(id)) {
                return project;
            }
        }

        E.checkArgument(false,
                        "The project with id '%s' does not exist in graphSpace '%s'",
                        id, graphSpace);
        return null;
    }

    @SuppressWarnings("unchecked")
    public List<HugeProject> listAllProjects(String graphSpace, long limit)
            throws IOException, ClassNotFoundException {
        List<HugeProject> result = new ArrayList<>();
        Map<String, String> projectMap =
                this.metaDriver.scanWithPrefix(projectListKey(graphSpace));

        for (Map.Entry<String, String> entry : projectMap.entrySet()) {
            if (limit >= 0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(entry.getValue(), Map.class);
            HugeProject project = HugeProject.fromMap(map);
            result.add(project);
        }

        return result;
    }

    private String userKey(String name) {
        // HUGEGRAPH/{cluster}/AUTH/USER/{user}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_AUTH,
                           META_PATH_USER,
                           name);
    }

    private String userListKey() {
        // HUGEGRAPH/{cluster}/AUTH/USER
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_AUTH,
                           META_PATH_USER);
    }

    private String authPrefix(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH);
    }

    private String groupKey(String group) {
        // HUGEGRAPH/{cluster}/AUTH/GROUP/{group}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_AUTH,
                           META_PATH_GROUP,
                           group);
    }

    private String groupListKey() {
        // HUGEGRAPH/{cluster}/AUTH/GROUP
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_AUTH,
                           META_PATH_GROUP);
    }

    private String roleKey(String graphSpace, String role) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ROLE/{role}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_ROLE,
                           role);
    }

    private String roleListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ROLE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_ROLE);
    }

    private String targetKey(String graphSpace, String target) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/TARGET/{target}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_TARGET,
                           target);
    }

    private String targetListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/TARGET
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_TARGET);
    }

    private String belongKey(String graphSpace, String belong) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG/{belong}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_BELONG,
                           belong);
    }

    private String belongListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_BELONG);
    }

    private String belongListKeyBySource(String graphSpace, String source) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG/{userName}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_BELONG,
                           source + "->");
    }

    private String accessKey(String graphSpace, String access) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS/{role->op->target}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_ACCESS,
                           access);
    }

    private String accessListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_ACCESS);
    }

    private String accessListKeyByRole(String graphSpace, String roleName) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS/{roleName}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_ACCESS,
                           roleName + "->");
    }

    private String accessListKeyByGroup(String graphSpace, String groupName) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS/{groupName}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_ACCESS,
                           groupName + "->");
    }

    private String projectKey(String graphSpace, String projectName) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/PROJECT/{projectName}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_PROJECT,
                           projectName);
    }

    private String projectListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/PROJECT
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_AUTH,
                           META_PATH_PROJECT);
    }
}
