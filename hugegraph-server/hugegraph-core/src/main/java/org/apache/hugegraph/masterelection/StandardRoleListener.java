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

package org.apache.hugegraph.masterelection;

import java.util.Objects;

import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class StandardRoleListener implements RoleListener {

    private static final Logger LOG = Log.logger(StandardRoleListener.class);

    private final TaskManager taskManager;

    private final GlobalMasterInfo roleInfo;

    private volatile boolean selfIsMaster;

    public StandardRoleListener(TaskManager taskManager,
                                GlobalMasterInfo roleInfo) {
        this.taskManager = taskManager;
        this.roleInfo = roleInfo;
        this.selfIsMaster = false;
    }

    @Override
    public void onAsRoleMaster(StateMachineContext context) {
        if (!selfIsMaster) {
            this.taskManager.onAsRoleMaster();
            LOG.info("Server {} change to master role", context.config().node());
        }
        this.updateMasterInfo(context);
        this.selfIsMaster = true;
    }

    @Override
    public void onAsRoleWorker(StateMachineContext context) {
        if (this.selfIsMaster) {
            this.taskManager.onAsRoleWorker();
            LOG.info("Server {} change to worker role", context.config().node());
        }
        this.updateMasterInfo(context);
        this.selfIsMaster = false;
    }

    @Override
    public void onAsRoleCandidate(StateMachineContext context) {
        // pass
    }

    @Override
    public void onAsRoleAbdication(StateMachineContext context) {
        if (this.selfIsMaster) {
            this.taskManager.onAsRoleWorker();
            LOG.info("Server {} change to worker role", context.config().node());
        }
        this.updateMasterInfo(context);
        this.selfIsMaster = false;
    }

    @Override
    public void error(StateMachineContext context, Throwable e) {
        LOG.error("Server {} exception occurred", context.config().node(), e);
    }

    @Override
    public void unknown(StateMachineContext context) {
        if (this.selfIsMaster) {
            this.taskManager.onAsRoleWorker();
            LOG.info("Server {} change to worker role", context.config().node());
        }
        this.updateMasterInfo(context);

        this.selfIsMaster = false;
    }

    public void updateMasterInfo(StateMachineContext context) {
        StateMachineContext.MasterServerInfo master = context.master();
        if (master == null) {
            this.roleInfo.resetMasterInfo();
            return;
        }

        boolean isMaster = Objects.equals(context.node(), master.node());
        this.roleInfo.masterInfo(isMaster, master.url());
    }
}
