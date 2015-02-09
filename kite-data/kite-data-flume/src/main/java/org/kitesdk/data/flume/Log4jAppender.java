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
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.PartitionStrategy;
import java.net.URL;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.flume.FlumeException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.StorageKey;
import org.kitesdk.data.spi.filesystem.PathConversion;

public class Log4jAppender extends org.apache.flume.clients.log4jappender.Log4jAppender {

  private static final String PARTITION_PREFIX = "kite.partition.";

  private String datasetRepositoryUri;
  private String datasetNamespace;
  private String datasetName;
  private boolean initialized;

  private PartitionStrategy partitionStrategy;
  private EntityAccessor<Object> accessor;
  private StorageKey key;

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

  public void setDatasetNamespace(String datasetNamespace) {
    this.datasetNamespace = datasetNamespace;
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
        URI datasetUri;
        if (datasetNamespace == null) {
          datasetUri = new URIBuilder(datasetRepositoryUri, URIBuilder.NAMESPACE_DEFAULT, datasetName).build();
        } else {
          datasetUri = new URIBuilder(datasetRepositoryUri, datasetNamespace, datasetName).build();
        }
        Dataset<Object> dataset = Datasets.load(datasetUri, Object.class);
        if (dataset.getDescriptor().isPartitioned()) {
          partitionStrategy = dataset.getDescriptor().getPartitionStrategy();
          accessor = DataModelUtil.accessor(
              dataset.getType(), dataset.getDescriptor().getSchema());
          key = new StorageKey(partitionStrategy);
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
      key.reuseFor(message, accessor);
      int i = 0;
      for (FieldPartitioner fp :
          Accessor.getDefault().getFieldPartitioners(partitionStrategy)) {
        hdrs.put(PARTITION_PREFIX + fp.getName(),
            PathConversion.valueToString(fp, key.get(i++)));
      }
    }
  }
}
