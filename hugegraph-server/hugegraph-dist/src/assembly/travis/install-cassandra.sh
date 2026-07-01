#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -ev

CASS_DOWNLOAD_ADDRESS="https://archive.apache.org/dist/cassandra"
CASS_VERSION="4.0.10"
CASS_PACKAGE="apache-cassandra-${CASS_VERSION}"
CASS_TAR="${CASS_PACKAGE}-bin.tar.gz"
CASS_CONF="${CASS_PACKAGE}/conf/cassandra.yaml"

# download cassandra
if [ ! -f $HOME/downloads/${CASS_TAR} ]; then
  wget -q -O $HOME/downloads/${CASS_TAR} ${CASS_DOWNLOAD_ADDRESS}/${CASS_VERSION}/${CASS_TAR}
fi

# decompress cassandra
cp $HOME/downloads/${CASS_TAR} ${CASS_TAR} && tar -xzf ${CASS_TAR}

# using tmpfs for the Cassandra data directory reduces travis test runtime
sudo mkdir /mnt/ramdisk
sudo mount -t tmpfs -o size=1024m tmpfs /mnt/ramdisk
sudo ln -s /mnt/ramdisk $CASS_PACKAGE/data

# config cassandra
sed -i "s/batch_size_warn_threshold_in_kb:.*/batch_size_warn_threshold_in_kb: 10240/g" $CASS_CONF
sed -i "s/batch_size_fail_threshold_in_kb:.*/batch_size_fail_threshold_in_kb: 10240/g" $CASS_CONF

# start cassandra service
sh ${CASS_PACKAGE}/bin/cassandra
