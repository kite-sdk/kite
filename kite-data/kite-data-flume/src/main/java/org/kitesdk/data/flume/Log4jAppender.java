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
package org.kitesdk.data.flume;

import java.net.URI;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import java.net.URL;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.flume.FlumeException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.URIBuilder;
import org.kitesdk.data.spi.filesystem.PathConversion;

public class Log4jAppender extends org.apache.flume.clients.log4jappender.Log4jAppender {

  private static final String PARTITION_PREFIX = "kite.partition.";

  private String datasetRepositoryUri;
  private String datasetName;
  private boolean initialized;
  
  private PartitionStrategy partitionStrategy;
  private PartitionKey key;

  public Log4jAppender() {
    super();
    setAvroReflectionEnabled(true);
  }

  /**
   * Sets the hostname and port. Even if these are passed the
   * <tt>activateOptions()</tt> function must be called before calling
   * <tt>append()</tt>, else <tt>append()</tt> will throw an Exception.
   * @param hostname The first hop where the client should connect to.
   * @param port The port to connect on the host.
   *
   */
  public Log4jAppender(String hostname, int port) {
    super(hostname, port);
    setAvroReflectionEnabled(true);
  }

  @Override
  public boolean requiresLayout() {
    // We don't support use of a layout
    return false;
  }

  /**
   * @deprecated Use datasetRepositoryUri with a 'repo:' URI.
   */
  @Deprecated
  public void setDatasetRepositoryClass(String datasetRepositoryClass) {
    throw new UnsupportedOperationException("datasetRepositoryClass is no longer " +
        "supported. Use datasetRepositoryUri with a 'repo:' URI.");
  }

  public void setDatasetRepositoryUri(String datasetRepositoryUri) {
    this.datasetRepositoryUri = datasetRepositoryUri;
  }

  public void setDatasetName(String datasetName) {
    this.datasetName = datasetName;
  }

  @Override
  @SuppressWarnings({"unchecked", "deprecation"})
  protected void populateAvroHeaders(Map<String, String> hdrs, Schema schema,
      Object message) {
    if (!initialized) {
      // initialize here rather than in activateOptions to avoid initialization
      // cycle in Configuration and log4j
      try {
        URI datasetUri = new URIBuilder(datasetRepositoryUri, datasetName).build();
        Dataset dataset = Datasets.load(datasetUri);
        if (dataset.getDescriptor().isPartitioned()) {
          partitionStrategy = dataset.getDescriptor().getPartitionStrategy();
        }
        URL schemaUrl = dataset.getDescriptor().getSchemaUrl();
        if (schemaUrl != null) {
          setAvroSchemaUrl(schemaUrl.toExternalForm());
        }
      } catch (Exception e) {
        throw new FlumeException(e);
      } finally {
        initialized = true;
      }
    }
    super.populateAvroHeaders(hdrs, schema, message);
    if (partitionStrategy != null) {
      key = PartitionKey.partitionKeyForEntity(partitionStrategy, message, key);
      int i = 0;
      for (FieldPartitioner fp : partitionStrategy.getFieldPartitioners()) {
        hdrs.put(PARTITION_PREFIX + fp.getName(),
            PathConversion.valueToString(fp, key.get(i++)));
      }
    }
  }
}
