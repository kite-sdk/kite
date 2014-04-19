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

bin="$1"

if [ -z "${bin}" ]; then
  bin=`dirname $0`
  bin=`cd ${bin} && pwd`
fi

# Find paths to our dependency systems. If they are unset, use CDH defaults.

if [ -z "${HADOOP_COMMON_HOME}" ]; then
  if [ -n "${HADOOP_HOME}" ]; then
    HADOOP_COMMON_HOME=${HADOOP_HOME}
  else
    if [ -d "/usr/lib/hadoop" ]; then
      HADOOP_COMMON_HOME=/usr/lib/hadoop
    # more guesses can go here...
    fi
  fi
fi

if [ -z "${HADOOP_MAPRED_HOME}" ]; then
  HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce
  if [ ! -d "${HADOOP_MAPRED_HOME}" ]; then
    if [ -n "${HADOOP_HOME}" ]; then
      HADOOP_MAPRED_HOME=${HADOOP_HOME}
    else
      HADOOP_MAPRED_HOME=${HADOOP_COMMON_HOME}/../hadoop-mapreduce
    fi
  fi
fi

# We are setting HADOOP_HOME to HADOOP_COMMON_HOME if it is not set
# so that hcat script works correctly on BigTop
if [ -z "${HADOOP_HOME}" ]; then
  if [ -n "${HADOOP_COMMON_HOME}" ]; then
     HADOOP_HOME=${HADOOP_COMMON_HOME}
     export HADOOP_HOME
  fi
fi

if [ -z "${HBASE_HOME}" ]; then
  if [ -d "/usr/lib/hbase" ]; then
    HBASE_HOME=/usr/lib/hbase
  else
    HBASE_HOME=${HADOOP_COMMON_HOME}/../hbase
  fi
fi
if [ -z "${HCAT_HOME}" ]; then
  if [ -d "/usr/lib/hive-hcatalog" ]; then
    HCAT_HOME=/usr/lib/hive-hcatalog
  elif [ -d "/usr/lib/hcatalog" ]; then
    HCAT_HOME=/usr/lib/hcatalog
  else
    HCAT_HOME=${HADOOP_COMMON_HOME}/../hive-hcatalog
    if [ ! -d ${HCAT_HOME} ]; then
       HCAT_HOME=${HADOOP_COMMON_HOME}/../hcatalog
    fi
  fi
fi

# Check: If we can't find our dependencies, give up here.
if [ ! -d "${HADOOP_COMMON_HOME}" ]; then
  echo "WARNING: Cannot find Hadoop installation!"
fi
# if [ ! -d "${HADOOP_MAPRED_HOME}" ]; then
#   echo "Error: $HADOOP_MAPRED_HOME does not exist!"
#   echo 'Please set $HADOOP_MAPRED_HOME to the root of your Hadoop MapReduce installation.'
#   exit 1
# fi

function add_to_classpath() {
  dir=$1
  for f in $dir/*.jar; do
    KITE_CLASSPATH=${KITE_CLASSPATH}:$f;
  done

  export KITE_CLASSPATH
}

# Add dependencies to classpath.
KITE_CLASSPATH=""

# Add HBase to dependency list
if [ -e "$HBASE_HOME/bin/hbase" ]; then
  TMP_KITE_CLASSPATH=${KITE_CLASSPATH}:`$HBASE_HOME/bin/hbase classpath`
  KITE_CLASSPATH=${TMP_KITE_CLASSPATH}
fi

# Add HCatalog to dependency list
if [ -e "${HCAT_HOME}/bin/hcat" ]; then
  TMP_KITE_CLASSPATH=${KITE_CLASSPATH}:`${HCAT_HOME}/bin/hcat -classpath`
  if [ -z "${HIVE_CONF_DIR}" ]; then
    TMP_KITE_CLASSPATH=${TMP_KITE_CLASSPATH}:${HIVE_CONF_DIR}
  fi
  KITE_CLASSPATH=${TMP_KITE_CLASSPATH}
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

export HADOOP_CLASSPATH
export HADOOP_COMMON_HOME
export HADOOP_MAPRED_HOME
export HBASE_HOME
export HCAT_HOME
export HIVE_CONF_DIR

if [ -x "$HADOOP_COMMON_HOME/bin/hadoop" ]; then
  exec ${HADOOP_COMMON_HOME}/bin/hadoop jar "$0" $flags "$@"
else
  # TODO: Add a stand-alone hadoop bundle
  echo "Cannot find hadoop, attempting to run without it"
  exec java $flags -jar "$0" "$@"
fi

# jar contents follows
