/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.crunch;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.ReadableData;
import org.apache.crunch.Source;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.impl.mr.run.CrunchMapper;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.CrunchInputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;
import org.kitesdk.data.spi.LastModifiedAccessor;
import org.kitesdk.data.spi.SizeAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DatasetSourceTarget<E> extends DatasetTarget<E> implements ReadableSourceTarget<E> {

  private static final Logger LOG = LoggerFactory
      .getLogger(DatasetSourceTarget.class);

  private View<E> view;
  private FormatBundle formatBundle;
  private AvroType<E> avroType;

  @SuppressWarnings("unchecked")
  public DatasetSourceTarget(View<E> view, Class<E> type) {
    super(view);

    this.view = view;

    Configuration temp = new Configuration(false /* use an empty conf */ );
    DatasetKeyInputFormat.configure(temp).readFrom(view);
    this.formatBundle = inputBundle(temp);

    // the following is only needed for input splits that are not instances of FileSplit
    formatBundle.set(RuntimeParameters.DISABLE_COMBINE_FILE, "true");

    if (type.isAssignableFrom(GenericData.Record.class)) {
      this.avroType = (AvroType<E>) Avros.generics(
          view.getDataset().getDescriptor().getSchema());
    } else {
      this.avroType = Avros.records(type);
    }
  }

  public DatasetSourceTarget(URI uri, Class<E> type) {
    this(Datasets.<E, View<E>>load(uri), type);
  }

  @Override
  public Source<E> inputConf(String key, String value) {
    formatBundle.set(key, value);
    return this;
  }

  @Override
  public PType<E> getType() {
    return avroType;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter() {
    return new KeyConverter<E>(avroType);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configureSource(Job job, int inputId) throws IOException {
    Configuration conf = job.getConfiguration();
    if (inputId == -1) {
      job.setMapperClass(CrunchMapper.class);
      job.setInputFormatClass(formatBundle.getFormatClass());
      formatBundle.configure(conf);
    } else {
      Path dummy = new Path("/view/" + view.getDataset().getName());
      CrunchInputs.addInputPath(job, dummy, formatBundle, inputId);
    }
  }

  @Override
  public long getSize(Configuration configuration) {
    if (view instanceof SizeAccessor) {
      return ((SizeAccessor) view).getSize();
    }
    LOG.warn("Cannot determine size for view: " + toString());
    return 1000L * 1000L * 1000L; // fallback to HBase default size
  }

  @Override
  public long getLastModifiedAt(Configuration configuration) {
    if (view instanceof LastModifiedAccessor) {
      return ((LastModifiedAccessor) view).getLastModified();
    }
    LOG.warn("Cannot determine last modified time for source: " + toString());
    return -1;
  }

  @Override
  public Iterable<E> read(Configuration configuration) throws IOException {
    // TODO: what to do with Configuration? create new view?
    DatasetReader<E> reader = view.newReader();
    reader.open();
    return reader; // TODO: who calls close?
  }

  @Override
  public ReadableData<E> asReadable() {
    return new ReadableData<E>() {

      @Override
      public Set<SourceTarget<?>> getSourceTargets() {
        return ImmutableSet.<SourceTarget<?>>of(DatasetSourceTarget.this);
      }

      @Override
      public void configure(Configuration conf) {
        // TODO: optimize for file-based datasets by using distributed cache
      }

      @Override
      public Iterable<E> read(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException {
        DatasetReader<E> reader = view.newReader();
        reader.open();
        return reader;
      }
    };
  }

  @Override
  public SourceTarget<E> conf(String key, String value) {
    inputConf(key, value);
    outputConf(key, value);
    return this;
  }

  /**
   * Builds a FormatBundle for DatasetKeyInputFormat by copying a temp config.
   *
   * All properties will be copied from the temporary configuration
   *
   * @param conf A Configuration that will be copied
   * @return a FormatBundle with the contents of conf
   */
  private static FormatBundle<DatasetKeyInputFormat> inputBundle(Configuration conf) {
    FormatBundle<DatasetKeyInputFormat> bundle = FormatBundle
        .forInput(DatasetKeyInputFormat.class);
    for (Map.Entry<String, String> entry : conf) {
      bundle.set(entry.getKey(), entry.getValue());
    }
    return bundle;
  }
}
