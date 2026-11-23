#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start Solr in background
solr-foreground &
SOLR_PID=$!

# Wait for Solr to start
sleep 20

# Create the ranger_audits core (standalone mode, not SolrCloud), using
# Ranger's own audit configset (managed-schema defines evtTime, reqUser,
# etc. — Solr's generic default configset does not, which is what causes
# "sort param field can't be found: evtTime" in the Ranger Admin UI).
echo "Creating ranger_audits core..."
/opt/solr/bin/solr create_core -c ranger_audits -d /ranger-audits-conf

# Wait for Solr process
wait $SOLR_PID
