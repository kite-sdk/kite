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
package com.cloudera.cdk.data.crunch;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.avro.mapred.AvroJob;
import org.apache.crunch.io.CompositePathIterable;
import org.apache.crunch.io.CrunchInputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.SourceTargetHelper;
import org.apache.crunch.io.avro.AvroFileReaderFactory;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroInputFormat;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * A variant of {@link org.apache.crunch.io.avro.AvroFileSource} for multiple paths.
 */
class MultiAvroFileSource<T> implements ReadableSource<T> {

  protected final List<Path> paths;
  protected final PType<T> ptype;
  protected final FormatBundle<? extends InputFormat> inputBundle;

  private static <S> FormatBundle getBundle(AvroType<S> ptype) {
    FormatBundle bundle = FormatBundle.forInput(AvroInputFormat.class)
        .set(AvroJob.INPUT_IS_REFLECT, String.valueOf(ptype.hasReflect()))
        .set(AvroJob.INPUT_SCHEMA, ptype.getSchema().toString())
        .set(Avros.REFLECT_DATA_FACTORY_CLASS, Avros.REFLECT_DATA_FACTORY.getClass().getName());
    return bundle;
  }

  public MultiAvroFileSource(List<Path> paths, AvroType<T> ptype) {
    this.paths = paths;
    this.ptype = ptype;
    this.inputBundle = getBundle(ptype);
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    if (inputId == -1) {
      for (Path path : paths) {
        FileInputFormat.addInputPath(job, path);
      }
      job.setInputFormatClass(inputBundle.getFormatClass());
      inputBundle.configure(job.getConfiguration());
    } else {
      for (Path path : paths) {
        CrunchInputs.addInputPath(job, path, inputBundle, inputId);
      }
    }
  }

  @Override
  public PType<T> getType() {
    return ptype;
  }

  @Override
  public long getSize(Configuration configuration) {
    long size = 0;
    for (Path path : paths) {
      try {
        size += SourceTargetHelper.getPathSize(configuration, path);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to get the file size of:" + path, e);
      }
    }
    return size;
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    List<Iterable<T>> iterables = Lists.newArrayList();
    for (Path path : paths) {
      FileSystem fs = path.getFileSystem(conf);
      iterables.add(CompositePathIterable.create(fs, path, new AvroFileReaderFactory<T>(
          (AvroType<T>) ptype)));
    }
    return Iterables.concat(iterables);
  }

}
