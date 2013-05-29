package com.cloudera.cdk.flume;

import com.cloudera.data.DatasetRepository;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URL;
import org.apache.flume.FlumeException;

public class Log4jAppender extends org.apache.flume.clients.log4jappender.Log4jAppender {

  private String datasetRepositoryClass;
  private String datasetRepositoryUri;
  private String datasetName;

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
  public void activateOptions() throws FlumeException {
    try {
      Class<?> c = Class.forName(datasetRepositoryClass);
      Constructor<?> cons = c.getConstructor(URI.class);
      DatasetRepository repo = (DatasetRepository)
          cons.newInstance(new URI(datasetRepositoryUri));

      URL schemaUrl = repo.get(datasetName).getDescriptor().getSchemaUrl();
      if (schemaUrl != null) {
        setAvroSchemaUrl(schemaUrl.toExternalForm());
      }
    } catch (Exception e) {
      throw new FlumeException(e);
    }

    super.activateOptions();
  }
}
