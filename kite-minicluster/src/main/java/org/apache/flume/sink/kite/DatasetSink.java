/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.kite;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.kite.DatasetSinkConstants;
import org.apache.flume.sink.kite.KerberosUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.Registration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Experimental sink that writes events to a Kite Dataset. This sink will
 * deserialize the body of each incoming event and store the resulting record
 * in a Kite Dataset. It determines target Dataset by opening a repository URI,
 * {@code kite.repo.uri}, and loading a Dataset by name,
 * {@code kite.dataset.name}.
 */
public class DatasetSink extends AbstractSink implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(org.apache.flume.sink.kite.DatasetSink.class);

  static Configuration conf = new Configuration();

  private String datasetName = null;
  private long batchSize = DatasetSinkConstants.DEFAULT_BATCH_SIZE;

  private URI target = null;
  private Schema targetSchema = null;
  private DatasetWriter<GenericRecord> writer = null;
  private UserGroupInformation login = null;
  private SinkCounter counter = null;

  // for rolling files at a given interval
  private int rollIntervalS = DatasetSinkConstants.DEFAULT_ROLL_INTERVAL;
  private long lastRolledMs = 0l;

  // for working with avro serialized records
  private GenericRecord datum = null;
  // TODO: remove this after PARQUET-62 is released
  private boolean reuseDatum = true;
  private BinaryDecoder decoder = null;
  private LoadingCache<Schema, DatumReader<GenericRecord>> readers =
      CacheBuilder.newBuilder()
      .build(new CacheLoader<Schema, DatumReader<GenericRecord>>() {
        @Override
        public DatumReader<GenericRecord> load(Schema schema) {
          // must use the target dataset's schema for reading to ensure the
          // records are able to be stored using it
          return new GenericDatumReader<GenericRecord>(
              schema, targetSchema);
        }
      });
  private static LoadingCache<String, Schema> schemasFromLiteral = CacheBuilder
      .newBuilder()
      .build(new CacheLoader<String, Schema>() {
        @Override
        public Schema load(String literal) {
          Preconditions.checkNotNull(literal,
              "Schema literal cannot be null without a Schema URL");
          return new Schema.Parser().parse(literal);
        }
      });
  private static LoadingCache<String, Schema> schemasFromURL = CacheBuilder
      .newBuilder()
      .build(new CacheLoader<String, Schema>() {
        @Override
        public Schema load(String url) throws IOException {
          Schema.Parser parser = new Schema.Parser();
          InputStream is = null;
          try {
            FileSystem fs = FileSystem.get(URI.create(url), conf);
            if (url.toLowerCase().startsWith("hdfs:/")) {
              is = fs.open(new Path(url));
            } else {
              is = new URL(url).openStream();
            }
            return parser.parse(is);
          } finally {
            if (is != null) {
              is.close();
            }
          }
        }
      });

  protected List<String> allowedFormats() {
    return Lists.newArrayList("avro", "parquet");
  }

  @Override
  public void configure(Context context) {
    // initialize login credentials
    this.login = KerberosUtil.login(
        context.getString(DatasetSinkConstants.AUTH_PRINCIPAL),
        context.getString(DatasetSinkConstants.AUTH_KEYTAB));
    String effectiveUser =
        context.getString(DatasetSinkConstants.AUTH_PROXY_USER);
    if (effectiveUser != null) {
      this.login = KerberosUtil.proxyAs(effectiveUser, login);
    }

    String datasetURI = context.getString(
        DatasetSinkConstants.CONFIG_KITE_DATASET_URI);
    if (datasetURI != null) {
      this.target = URI.create(datasetURI);
      this.datasetName = uriToName(target);
    } else {
      String repositoryURI = context.getString(
          DatasetSinkConstants.CONFIG_KITE_REPO_URI);
      Preconditions.checkNotNull(repositoryURI, "Repository URI is missing");
      this.datasetName = context.getString(
          DatasetSinkConstants.CONFIG_KITE_DATASET_NAME);
      Preconditions.checkNotNull(datasetName, "Dataset name is missing");

      this.target = new URIBuilder(repositoryURI, URIBuilder.NAMESPACE_DEFAULT,
          datasetName).build();
    }

    this.setName(target.toString());

    // other configuration
    this.batchSize = context.getLong(
        DatasetSinkConstants.CONFIG_KITE_BATCH_SIZE,
        DatasetSinkConstants.DEFAULT_BATCH_SIZE);
    this.rollIntervalS = context.getInteger(
        DatasetSinkConstants.CONFIG_KITE_ROLL_INTERVAL,
        DatasetSinkConstants.DEFAULT_ROLL_INTERVAL);

    this.counter = new SinkCounter(datasetName);
  }

  @Override
  public synchronized void start() {
    this.lastRolledMs = System.currentTimeMillis();
    counter.start();
    // signal that this sink is ready to process
    LOG.info("Started DatasetSink " + getName());
    super.start();
  }

  /**
   * Causes the sink to roll at the next {@link #process()} call.
   */
  @VisibleForTesting
  public void roll() {
    this.lastRolledMs = 0l;
  }

  @Override
  public synchronized void stop() {
    counter.stop();

    if (writer != null) {
      // any write problems invalidate the writer, which is immediately closed
      writer.close();
      this.writer = null;
      this.lastRolledMs = System.currentTimeMillis();
    }

    // signal that this sink has stopped
    LOG.info("Stopped dataset sink: " + getName());
    super.stop();
  }

  @Override
  public Status process() throws EventDeliveryException {
    if (writer == null) {
      try {
        this.writer = newWriter(login, target);
      } catch (DatasetException e) {
        // DatasetException includes DatasetNotFoundException
        throw new EventDeliveryException(
            "Cannot write to " + getName(), e);
      }
    }

    // handle file rolling
    if ((System.currentTimeMillis() - lastRolledMs) / 1000 > rollIntervalS) {
      // close the current writer and get a new one
      writer.close();
      this.writer = newWriter(login, target);
      this.lastRolledMs = System.currentTimeMillis();
      LOG.info("Rolled writer for " + getName());
    }

    Channel channel = getChannel();
    Transaction transaction = null;
    try {
      long processedEvents = 0;

      transaction = channel.getTransaction();
      transaction.begin();
      for (; processedEvents < batchSize; processedEvents += 1) {
        Event event = channel.take();
        if (event == null) {
          // no events available in the channel
          break;
        }

        this.datum = deserialize(event, reuseDatum ? datum : null);

        // writeEncoded would be an optimization in some cases, but HBase
        // will not support it and partitioned Datasets need to get partition
        // info from the entity Object. We may be able to avoid the
        // serialization round-trip otherwise.
        writer.write(datum);
      }

      // TODO: Add option to sync, depends on CDK-203
      if (writer instanceof Flushable) {
        ((Flushable) writer).flush();
      }

      // commit after data has been written and flushed
      transaction.commit();

      if (processedEvents == 0) {
        counter.incrementBatchEmptyCount();
        return Status.BACKOFF;
      } else if (processedEvents < batchSize) {
        counter.incrementBatchUnderflowCount();
      } else {
        counter.incrementBatchCompleteCount();
      }

      counter.addToEventDrainSuccessCount(processedEvents);

      return Status.READY;

    } catch (Throwable th) {
      // catch-all for any unhandled Throwable so that the transaction is
      // correctly rolled back.
      if (transaction != null) {
        try {
          transaction.rollback();
        } catch (Exception ex) {
          LOG.error("Transaction rollback failed", ex);
          throw Throwables.propagate(ex);
        }
      }

      // close the writer and remove the its reference
      writer.close();
      this.writer = null;
      this.lastRolledMs = System.currentTimeMillis();

      // handle the exception
      Throwables.propagateIfInstanceOf(th, Error.class);
      Throwables.propagateIfInstanceOf(th, EventDeliveryException.class);
      throw new EventDeliveryException(th);

    } finally {
      if (transaction != null) {
        transaction.close();
      }
    }
  }

  private DatasetWriter<GenericRecord> newWriter(
      final UserGroupInformation login, final URI uri) {
    View<GenericRecord> view = KerberosUtil.runPrivileged(login,
        new PrivilegedExceptionAction<Dataset<GenericRecord>>() {
          @Override
          public Dataset<GenericRecord> run() {
            return Datasets.load(uri);
          }
        });

    DatasetDescriptor descriptor = view.getDataset().getDescriptor();
    String formatName = descriptor.getFormat().getName();
    Preconditions.checkArgument(allowedFormats().contains(formatName),
        "Unsupported format: " + formatName);

    Schema newSchema = descriptor.getSchema();
    if (targetSchema == null || !newSchema.equals(targetSchema)) {
      this.targetSchema = descriptor.getSchema();
      // target dataset schema has changed, invalidate all readers based on it
      readers.invalidateAll();
    }

    this.reuseDatum = !("parquet".equals(formatName));
    this.datasetName = view.getDataset().getName();

    return view.newWriter();
  }

  /**
   * Not thread-safe.
   *
   * @param event
   * @param reuse
   * @return
   */
  private GenericRecord deserialize(Event event, GenericRecord reuse)
      throws EventDeliveryException {
    decoder = DecoderFactory.get().binaryDecoder(event.getBody(), decoder);
    // no checked exception is thrown in the CacheLoader
    DatumReader<GenericRecord> reader = readers.getUnchecked(schema(event));
    try {
      return reader.read(reuse, decoder);
    } catch (IOException ex) {
      throw new EventDeliveryException("Cannot deserialize event", ex);
    }
  }

  private static Schema schema(Event event) throws EventDeliveryException {
    Map<String, String> headers = event.getHeaders();
    String schemaURL = headers.get(
        DatasetSinkConstants.AVRO_SCHEMA_URL_HEADER);
    try {
      if (headers.get(DatasetSinkConstants.AVRO_SCHEMA_URL_HEADER) != null) {
        return schemasFromURL.get(schemaURL);
      } else {
        return schemasFromLiteral.get(
            headers.get(DatasetSinkConstants.AVRO_SCHEMA_LITERAL_HEADER));
      }
    } catch (ExecutionException ex) {
      throw new EventDeliveryException("Cannot get schema", ex.getCause());
    }
  }

  private static String uriToName(URI uri) {
    return Registration.lookupDatasetUri(URI.create(
        uri.getRawSchemeSpecificPart())).second().get("dataset");
  }
}
