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
package com.cloudera.data.filesystem;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetException;
import com.cloudera.data.Formats;
import com.google.common.annotations.Beta;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.Target;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.io.avro.AvroFileTarget;
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

  public static <E> ReadableSource<E> asSource(Dataset dataset, Class<E> type) {
    if (dataset instanceof FileSystemDataset) {
      FileSystemDataset fsDataset = (FileSystemDataset) dataset;
      List<Path> paths = Lists.newArrayList();

      try {
        fsDataset.accumulateDatafilePaths(fsDataset.getDirectory(), paths);
      } catch (IOException e) {
        throw new DatasetException("Unable to retrieve data file list for directory " +
            fsDataset.getDirectory(), e);
      }

      AvroType<E> avroType;
      if (type.isAssignableFrom(GenericData.Record.class)) {
        avroType = (AvroType<E>) Avros.generics(fsDataset.getDescriptor().getSchema());
      } else {
        avroType = Avros.records(type);
      }
      return new AvroFileSource<E>(paths.get(0), avroType); // TODO: use all paths
    }
    return null;
  }

  public static Target asTarget(Dataset dataset) {
    if (dataset instanceof FileSystemDataset) {
      FileSystemDataset fsDataset = (FileSystemDataset) dataset;
      if (fsDataset.getDescriptor().getFormat() == Formats.PARQUET) {
        throw new UnsupportedOperationException("Parquet is not supported.");
      }
      return new AvroFileTarget(fsDataset.getDirectory());
    }
    return null;
  }
}
