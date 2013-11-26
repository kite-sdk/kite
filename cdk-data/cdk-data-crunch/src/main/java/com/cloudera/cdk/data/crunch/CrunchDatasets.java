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

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetException;
import com.cloudera.cdk.data.Formats;
import com.cloudera.cdk.data.filesystem.impl.Accessor;
import com.google.common.annotations.Beta;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.Target;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.io.parquet.AvroParquetFileTarget;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;

/**
 * <p>
 * A helper class for exposing a filesystem-based dataset as a Crunch
 * {@link ReadableSource} or {@link Target}.
 * </p>
 */
@Beta
public class CrunchDatasets {

  /**
   * <p>
   * Expose the given {@link Dataset} as a Crunch {@link ReadableSource}.
   * </p>
   * @param dataset the dataset to read from
   * @param type    the Java type of the entities in the dataset
   * @param <E>     the type of entity produced by the source
   * @return the {@link ReadableSource}, or <code>null</code> if the dataset is not
   * filesystem-based.
   */
  @SuppressWarnings("unchecked")
  public static <E> ReadableSource<E> asSource(Dataset<E> dataset, Class<E> type) {
    Path directory = Accessor.getDefault().getDirectory(dataset);
    if (directory != null) {
      List<Path> paths = Lists.newArrayList(
          Accessor.getDefault().getPathIterator(dataset));

      AvroType<E> avroType;
      if (type.isAssignableFrom(GenericData.Record.class)) {
        avroType = (AvroType<E>) Avros.generics(dataset.getDescriptor().getSchema());
      } else {
        avroType = Avros.records(type);
      }
      if (dataset.getDescriptor().getFormat() == Formats.PARQUET) {
        return new AvroParquetFileSource<E>(paths, avroType);
      }
      return new AvroFileSource<E>(paths, avroType);
    }
    return null;
  }

  /**
   * <p>
   * Expose the given {@link Dataset} as a Crunch {@link Target}.
   * </p>
   * @param dataset the dataset to write to
   * @return the {@link Target}, or <code>null</code> if the dataset is not
   * filesystem-based.
   */
  public static Target asTarget(Dataset dataset) {
    Path directory = Accessor.getDefault().getDirectory(dataset);
    if (directory != null) {
      if (dataset.getDescriptor().getFormat() == Formats.PARQUET) {
        return new AvroParquetFileTarget(directory);
      }
      return new AvroFileTarget(directory);
    }
    return null;
  }
}
