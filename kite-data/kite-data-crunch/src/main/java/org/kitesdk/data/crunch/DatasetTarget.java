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

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.Map;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.MapReduceTarget;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.kitesdk.data.View;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;

class DatasetTarget<E> implements MapReduceTarget {

  FormatBundle formatBundle;
  private URI datasetUri;

  public DatasetTarget(View<E> view) {
    Configuration temp = emptyConf();
    DatasetKeyOutputFormat.configure(temp).writeTo(view);
    this.formatBundle = outputBundle(temp);
    this.datasetUri = view.getDataset().getUri();
  }

  public DatasetTarget(URI uri) {
    Configuration temp = emptyConf();
    DatasetKeyOutputFormat.configure(temp).writeTo(uri);
    this.formatBundle = outputBundle(temp);
    this.datasetUri = uri;
  }

  @Override
  public Target outputConf(String key, String value) {
    formatBundle.set(key, value);
    return this;
  }

  @Override
  public boolean handleExisting(WriteMode writeMode, long l, Configuration entries) {
    // currently don't check for existing outputs
    return false;
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (!(ptype instanceof AvroType)) {
      return false;
    }
    handler.configure(this, ptype);
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Converter<?, ?, ?, ?> getConverter(PType<?> ptype) {
    return new KeyConverter<E>((AvroType<E>) ptype);
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> tpType) {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {

    Preconditions.checkNotNull(name, "Output name should not be null"); // see CRUNCH-82

    Converter converter = getConverter(ptype);
    Class<?> keyClass = converter.getKeyClass();
    Class<?> valueClass = Void.class;

    CrunchOutputs.addNamedOutput(job, name, formatBundle, keyClass, valueClass);
    job.setOutputFormatClass(formatBundle.getFormatClass());
    formatBundle.configure(job.getConfiguration());
  }

  private static Configuration emptyConf() {
    return new Configuration(false /* do not load defaults */ );
  }

  /**
   * Builds a FormatBundle for DatasetKeyOutputFormat by copying a temp config.
   *
   * All properties will be copied from the temporary configuration
   *
   * @param conf A Configuration that will be copied
   * @return a FormatBundle with the contents of conf
   */
  private static FormatBundle<DatasetKeyOutputFormat> outputBundle(Configuration conf) {
    FormatBundle<DatasetKeyOutputFormat> bundle = FormatBundle
        .forOutput(DatasetKeyOutputFormat.class);
    for (Map.Entry<String, String> entry : conf) {
      bundle.set(entry.getKey(), entry.getValue());
    }
    return bundle;
  }
  
  /**
   * Returns a brief description of this {@code DatasetTarget}. The exact details of the
   * representation are unspecified and subject to change, but the following may be regarded
   * as typical:
   * <p>
   * "Kite(dataset:hdfs://host/path/to/repo)"
   * 
   * @return  a string representation of the object.
   */
  @Override
  public String toString() {
    return new StringBuilder().append("Kite(").append(datasetUri).append(")").toString();
  }
  
}
