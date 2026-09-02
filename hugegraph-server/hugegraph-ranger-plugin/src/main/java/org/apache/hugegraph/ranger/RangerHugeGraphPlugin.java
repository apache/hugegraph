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

package org.apache.hugegraph.ranger;

import java.io.File;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.ResourceAuthorizer;
import org.apache.hugegraph.auth.ResourceType;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin wrapper around {@link RangerBasePlugin} that:
 * <ul>
 *   <li>Maps HugeGraph resource/permission concepts to Ranger access requests.</li>
 *   <li>Initialises from a {@code ranger-hugegraph-security.xml} config file or
 *       from the Ranger Admin pull-based refresh cycle.</li>
 * </ul>
 *
 * <h3>Ranger resource hierarchy for HugeGraph</h3>
 * <pre>
 *   graphspace  (e.g. "DEFAULT", "*")
 *   └─ graph    (e.g. "hugegraph", "*")
 *      └─ resource-type  (VERTEX, EDGE, SCHEMA, GREMLIN, …)
 *         └─ label        (vertex/edge label name, "*")
 * </pre>
 *
 * <h3>Access types registered in the Ranger service definition</h3>
 * <pre>
 *   read, write, delete, execute, admin
 * </pre>
 */
public class RangerHugeGraphPlugin extends RangerBasePlugin implements ResourceAuthorizer {

    private static final Logger LOG =
            LoggerFactory.getLogger(RangerHugeGraphPlugin.class);

    /** Resource key names used in the Ranger service definition. */
    public static final String RES_GRAPHSPACE    = "graphspace";
    public static final String RES_GRAPH         = "graph";
    public static final String RES_RESOURCE_TYPE = "resource-type";
    public static final String RES_LABEL         = "label";

    /** Ranger application-type that matches the service definition name. */
    public static final String APP_TYPE = "hugegraph";

    private final String configFile;

    public RangerHugeGraphPlugin(String serviceName, String configFile) {
        super(APP_TYPE, serviceName);
        this.configFile = configFile;
    }

    @Override
    public void init() {
        if (configFile != null && !configFile.isEmpty()) {
            File f = new File(configFile);
            if (f.exists()) {
                // Add as a Hadoop Configuration resource after the classpath
                // XML has been loaded — later resources override earlier ones,
                // so this replaces the bundled placeholder values (e.g.
                // ranger-admin-host) with the values from the external file.
                getConfig().addResource(new Path(f.getAbsolutePath()));
                LOG.info("Loaded Ranger config from {}", configFile);
            } else {
                LOG.warn("Ranger config file not found, using classpath defaults: {}",
                         configFile);
            }
        }
        super.init();
        // RangerBasePlugin only invokes the configured audit destinations
        // (file/Solr/...) when a result processor is set — without this,
        // isAccessAllowed() silently skips auditing entirely, no error.
        setResultProcessor(new RangerDefaultAuditHandler());
        LOG.info("RangerHugeGraphPlugin started for service '{}'",
                 getServiceName());
    }

    /**
     * Check whether {@code username} is allowed to perform {@code permission}
     * on the given resource path.
     *
     * @param username     authenticated HugeGraph user
     * @param graphSpace   graph space (or "*" for any)
     * @param graph        graph name (or "*" for any)
     * @param resourceType HugeGraph resource type
     * @param label        vertex/edge label (or "*" for any)
     * @param permission   requested HugePermission
     * @return true if the Ranger policy grants access
     */
    @Override
    public boolean isAllowed(String username,
                             String graphSpace,
                             String graph,
                             ResourceType resourceType,
                             String label,
                             HugePermission permission) {
        RangerAccessRequestImpl request = buildRequest(
                username, graphSpace, graph, resourceType, label, permission);

        RangerAccessResult result = isAccessAllowed(request);
        boolean allowed = result != null && result.getIsAllowed();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ranger check user={} gs={} graph={} type={} label={} " +
                      "access={} → {}",
                      username, graphSpace, graph, resourceType,
                      label, request.getAccessType(), allowed ? "ALLOW" : "DENY");
        }
        return allowed;
    }

    /**
     * Build the Ranger access request for a single permission check. Package-visible
     * for unit testing (constructing a real request without a live policy engine).
     */
    RangerAccessRequestImpl buildRequest(String username,
                                         String graphSpace,
                                         String graph,
                                         ResourceType resourceType,
                                         String label,
                                         HugePermission permission) {
        RangerAccessResourceImpl resource = buildResource(
                graphSpace, graph, resourceType, label);
        String accessType = hugePermToRangerAccess(permission);

        RangerAccessRequestImpl request = new RangerAccessRequestImpl(
                resource, accessType, username, null, null);
        request.setAccessTime(new Date());
        request.setRequestData(graphSpace + "/" + graph + "/" +
                               resourceType + "/" + label);
        // RangerDefaultAuditHandler populates the audited "Access Type"
        // column from getAction(), not getAccessType() — without this the
        // Ranger Admin UI audit log always shows a null Access Type.
        request.setAction(accessType);
        return request;
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private RangerAccessResourceImpl buildResource(String graphSpace,
                                                   String graph,
                                                   ResourceType resourceType,
                                                   String label) {
        RangerAccessResourceImpl res = new RangerAccessResourceImpl();
        res.setValue(RES_GRAPHSPACE, graphSpace);
        res.setValue(RES_GRAPH, graph);
        res.setValue(RES_RESOURCE_TYPE, resourceType.name().toLowerCase());
        res.setValue(RES_LABEL, label);
        return res;
    }

    static String hugePermToRangerAccess(HugePermission perm) {
        switch (perm) {
            case READ:    return "read";
            case WRITE:   return "write";
            case DELETE:  return "delete";
            case EXECUTE: return "execute";
            case ADMIN:   return "admin";
            default:      return "read";
        }
    }

    static HugePermission rangerAccessToHugePerm(String access) {
        switch (access.toLowerCase()) {
            case "read":    return HugePermission.READ;
            case "write":   return HugePermission.WRITE;
            case "delete":  return HugePermission.DELETE;
            case "execute": return HugePermission.EXECUTE;
            case "admin":   return HugePermission.ADMIN;
            default:        return HugePermission.NONE;
        }
    }
}
