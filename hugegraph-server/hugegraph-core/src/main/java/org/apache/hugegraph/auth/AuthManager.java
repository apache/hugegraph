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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.sasl.AuthenticationException;

import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.backend.id.Id;

public interface AuthManager {

    void init();

    boolean close();

    Id createUser(HugeUser user);

    Id updateUser(HugeUser user);

    HugeUser deleteUser(Id id);

    HugeUser findUser(String name);

    HugeUser getUser(Id id);

    List<HugeUser> listUsers(List<Id> ids);

    List<HugeUser> listAllUsers(long limit);

    Id createGroup(HugeGroup group);

    Id updateGroup(HugeGroup group);

    HugeGroup deleteGroup(Id id);

    HugeGroup getGroup(Id id);

    List<HugeGroup> listGroups(List<Id> ids);

    List<HugeGroup> listAllGroups(long limit);

    Id createTarget(HugeTarget target);

    Id updateTarget(HugeTarget target);

    HugeTarget deleteTarget(Id id);

    HugeTarget getTarget(Id id);

    List<HugeTarget> listTargets(List<Id> ids);

    List<HugeTarget> listAllTargets(long limit);

    Id createBelong(HugeBelong belong);

    Id updateBelong(HugeBelong belong);

    HugeBelong deleteBelong(Id id);

    HugeBelong getBelong(Id id);

    List<HugeBelong> listBelong(List<Id> ids);

    List<HugeBelong> listAllBelong(long limit);

    List<HugeBelong> listBelongByUser(Id user, long limit);

    List<HugeBelong> listBelongByGroup(Id group, long limit);

    Id createAccess(HugeAccess access);

    Id updateAccess(HugeAccess access);

    HugeAccess deleteAccess(Id id);

    HugeAccess getAccess(Id id);

    List<HugeAccess> listAccess(List<Id> ids);

    List<HugeAccess> listAllAccess(long limit);

    List<HugeAccess> listAccessByGroup(Id group, long limit);

    List<HugeAccess> listAccessByTarget(Id target, long limit);

    Id createProject(HugeProject project);

    HugeProject deleteProject(Id id);

    Id updateProject(HugeProject project);

    Id projectAddGraphs(Id id, Set<String> graphs);

    Id projectRemoveGraphs(Id id, Set<String> graphs);

    HugeProject getProject(Id id);

    List<HugeProject> listAllProject(long limit);

    HugeUser matchUser(String name, String password);

    RolePermission rolePermission(AuthElement element);

    String loginUser(String username, String password) throws AuthenticationException;

    String loginUser(String username, String password, long expire) throws AuthenticationException;

    void logoutUser(String token);

    UserWithRole validateUser(String username, String password);

    UserWithRole validateUser(String token);

    Set<String> listWhiteIPs();

    void setWhiteIPs(Set<String> whiteIpList);

    boolean getWhiteIpStatus();

    void enabledWhiteIpList(boolean status);

    Id createSpaceManager(String graphSpace, String owner);

    void deleteSpaceManager(String graphSpace, String owner);

    List<String> listSpaceManager(String graphSpace);

    boolean isSpaceManager(String owner);

    boolean isSpaceManager(String graphSpace, String owner);

    Id createSpaceMember(String graphSpace, String user);

    void deleteSpaceMember(String graphSpace, String user);

    List<String> listSpaceMember(String graphSpace);

    boolean isSpaceMember(String graphSpace, String user);

    Id createAdminManager(String user);

    void deleteAdminManager(String user);

    List<String> listAdminManager();

    boolean isAdminManager(String user);

    HugeGroup findGroup(String name);

    void setDefaultGraph(String graphSpace, String graph, String user);

    void unsetDefaultGraph(String graphSpace, String graph, String user);

    Map<String, Date> getDefaultGraph(String graphSpace, String user);

    Id createDefaultRole(String graphSpace, String owner,
                         HugeDefaultRole role, String graph);

    Id createSpaceDefaultRole(String graphSpace, String owner,
                              HugeDefaultRole role);

    boolean isDefaultRole(String graphSpace, String owner,
                          HugeDefaultRole role);

    boolean isDefaultRole(String graphSpace, String graph, String owner,
                          HugeDefaultRole role);

    void deleteDefaultRole(String graphSpace, String owner,
                           HugeDefaultRole role);

    void deleteDefaultRole(String graphSpace, String owner,
                           HugeDefaultRole role, String graph);
}
