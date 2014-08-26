/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.hbase.impl;

import org.kitesdk.data.hbase.HBaseDatasetRepository;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.Loadable;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.URIPattern;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

/**
 * A Loader implementation to register URIs for
 * {@link org.kitesdk.data.hbase.HBaseDatasetRepository}.
 */
public class Loader implements Loadable {

  @Override
  public void load() {
    Registration.register(
        new URIPattern("hbase::zk"),
        new URIPattern("hbase::zk/:dataset?namespace=default"),
        new OptionBuilder<DatasetRepository>() {
          @Override
          public DatasetRepository getFromOptions(Map<String, String> options) {
            Configuration conf = HBaseConfiguration.create(DefaultConfiguration.get());
            String[] hostsAndPort = parseHostsAndPort(options.get("zk"));
            conf.set(HConstants.ZOOKEEPER_QUORUM, hostsAndPort[0]);
            String port = hostsAndPort[1];
            if (port != null) {
              conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);
            }
            return new HBaseDatasetRepository.Builder().configuration(conf).build();
          }
        });
  }

  @VisibleForTesting
  public static String[] parseHostsAndPort(String zkQuorum) {
    List<String> hosts = Lists.newArrayList();
    String port = null;
    for (String hostPort : Splitter.on(',').split(zkQuorum)) {
      Iterator<String> split = Splitter.on(':').split(hostPort).iterator();
      hosts.add(split.next());
      if (split.hasNext()) {
        String p = split.next();
        if (port == null) {
          port = p;
        } else if (!port.equals(p)) {
          throw new IllegalArgumentException("Mismatched ports in " + zkQuorum);
        }
      }
    }
    return new String[] { Joiner.on(',').join(hosts), port };
  }
}
