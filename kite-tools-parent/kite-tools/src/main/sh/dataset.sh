#!/bin/bash
#
# Copyright 2011 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This is based on the Sqoop startup script (ASL 2.0)

function debug() {
  [ -n "$debug" ] && echo "$0 debug: $@"
}

bin=`dirname $0`
bin=`cd ${bin} && pwd`

# Find paths to our dependency systems. If they are unset, use CDH defaults.

if [ -z "${HADOOP_COMMON_HOME}" ]; then
  if [ -n "${HADOOP_HOME}" ]; then
    HADOOP_COMMON_HOME=${HADOOP_HOME}
  else
    if [ -d "/usr/lib/hadoop" ]; then
      HADOOP_COMMON_HOME=/usr/lib/hadoop
    elif [ -d "/opt/cloudera/parcels/CDH/lib/hadoop" ]; then
      HADOOP_COMMON_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
    fi
  fi
fi
debug "Using HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME}"

# We are setting HADOOP_HOME to HADOOP_COMMON_HOME if it is not set
# so that hcat script works correctly on BigTop
if [ -z "${HADOOP_HOME}" ]; then
  if [ -n "${HADOOP_COMMON_HOME}" ]; then
     HADOOP_HOME=${HADOOP_COMMON_HOME}
     export HADOOP_HOME
  fi
fi

if [ -z "${HADOOP_MAPRED_HOME}" ]; then
  HADOP_MAPRED_HOME=/usr/lib/hadoop-0.20-mapreduce
  if [ ! -d "${HADOOP_MAPRED_HOME}" ]; then
    HADOOP_MAPRED_HOME=${HADOOP_COMMON_HOME}/../hadoop-0.20-mapreduce
    if [ ! -d "${HADOOP_MAPRED_HOME}" ]; then
      HADOOP_MAPRED_HOME=${HADOOP_COMMON_HOME}/../hadoop-mapreduce
    fi
  fi
fi
debug "Using HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME}"

if [ -z "${HBASE_HOME}" ]; then
  if [ -d "/usr/lib/hbase" ]; then
    HBASE_HOME=/usr/lib/hbase
  else
    HBASE_HOME=${HADOOP_COMMON_HOME}/../hbase
  fi
fi
debug "Using HBASE_HOME=${HBASE_HOME}"

if [ -z "${HIVE_HOME}" ]; then
  if [ -d "/usr/lib/hive" ]; then
    HIVE_HOME=/usr/lib/hive
  else
    HIVE_HOME=${HADOOP_COMMON_HOME}/../hive
  fi
fi
debug "Using HIVE_HOME=${HIVE_HOME}"

if [ -z "${HIVE_CONF_DIR}" ]; then
  HIVE_CONF_DIR=/etc/hive/conf
  if [ ! -d "${HIVE_CONF_DIR}" ]; then
    HIVE_CONF_DIR=${HIVE_HOME}/conf
  fi
fi
debug "Using HIVE_CONF_DIR=${HIVE_CONF_DIR}"

# Add dependencies to classpath.
KITE_CLASSPATH=""

# Add HBase to dependency list
if [ -e "$HBASE_HOME/bin/hbase" ]; then
  TMP_KITE_CLASSPATH=${KITE_CLASSPATH}:`$HBASE_HOME/bin/hbase classpath`
  KITE_CLASSPATH=${TMP_KITE_CLASSPATH}
fi

if [ -d "$HIVE_HOME" ]; then
  # need to add lib/* to pick up the jars
  KITE_CLASSPATH=$HIVE_HOME/lib/*:$KITE_CLASSPATH
  if [ -d "$HIVE_CONF_DIR" ]; then
    # adding $HIVE_CONF_DIR/* prevents hive from finding hive-site.xml
    KITE_CLASSPATH=$HIVE_CONF_DIR:$KITE_CLASSPATH
  fi
fi

ZOOCFGDIR=${ZOOCFGDIR:-/etc/zookeeper}
if [ -d "${ZOOCFGDIR}" ]; then
  KITE_CLASSPATH=$ZOOCFGDIR:$KITE_CLASSPATH
fi

HADOOP_CLASSPATH="$0:${KITE_CLASSPATH}:${HADOOP_CLASSPATH}"
if [ ! -z "$KITE_USER_CLASSPATH" ]; then
  # User has elements to prepend to the classpath, forcibly overriding
  # Kite's own lib directories.
  export HADOOP_CLASSPATH="${KITE_USER_CLASSPATH}:${HADOOP_CLASSPATH}"
fi
debug "Using HADOOP_CLASSPATH=${HADOOP_CLASSPATH}"

export HADOOP_CLASSPATH
export HADOOP_COMMON_HOME
export HADOOP_MAPRED_HOME
export HBASE_HOME
export HIVE_CONF_DIR
export HIVE_HOME

if [ -x "$HADOOP_COMMON_HOME/bin/hadoop" ]; then
  exec ${HADOOP_COMMON_HOME}/bin/hadoop jar "$0" $flags org.kitesdk.cli.Main --dollar-zero "$0" "$@"
else
  echo "ERROR: Cannot find Hadoop installation!"
  echo "You can fix this warning by setting HADOOP_HOME"
fi

exit
# jar contents follows
