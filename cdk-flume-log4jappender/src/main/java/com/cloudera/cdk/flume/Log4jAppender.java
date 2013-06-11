package com.cloudera.cdk.flume;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetRepository;
import com.cloudera.data.FieldPartitioner;
import com.cloudera.data.PartitionKey;
import com.cloudera.data.PartitionStrategy;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.flume.FlumeException;

public class Log4jAppender extends org.apache.flume.clients.log4jappender.Log4jAppender {

  private static final String PARTITION_PREFIX = "cdk.partition.";

  private String datasetRepositoryClass;
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

  public void setDatasetRepositoryClass(String datasetRepositoryClass) {
    this.datasetRepositoryClass = datasetRepositoryClass;
  }

  public void setDatasetRepositoryUri(String datasetRepositoryUri) {
    this.datasetRepositoryUri = datasetRepositoryUri;
  }

  public void setDatasetName(String datasetName) {
    this.datasetName = datasetName;
  }

  @Override
  protected void populateAvroHeaders(Map<String, String> hdrs, Schema schema,
      Object message) {
    if (!initialized) {
      // initialize here rather than in activateOptions to avoid initialization
      // cycle in Configuration and log4j
      try {
        Class<?> c = Class.forName(datasetRepositoryClass);
        Constructor<?> cons = c.getConstructor(URI.class);
        DatasetRepository repo = (DatasetRepository)
            cons.newInstance(new URI(datasetRepositoryUri));

        Dataset dataset = repo.get(datasetName);
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
      key = partitionStrategy.partitionKeyForEntity(message, key);
      int i = 0;
      for (FieldPartitioner fp : partitionStrategy.getFieldPartitioners()) {
        hdrs.put(PARTITION_PREFIX + fp.getName(), fp.valueToString(key.get(i++)));
      }
    }
  }
}
