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
package com.cloudera.cdk.data.filesystem.impl;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.View;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * <p>
 * Class to enforce "friend" access to internal methods in
 * {@link com.cloudera.cdk.data.filesystem} classes that are not a part of the public
 * API.
 * </p>
 * <p>
 * This technique is described in detail in "Practical API Design" by
 * Jaroslav Tulach.
 * </p>
 */
public abstract class Accessor {
  private static volatile Accessor DEFAULT;
  public static Accessor getDefault() {
    Accessor a = DEFAULT;
    if (a != null) {
      return a;
    }
    try {
      Class.forName(FileSystemDatasetRepository.class.getName(), true,
          FileSystemDatasetRepository.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return DEFAULT;
  }

  public static void setDefault(Accessor accessor) {
    if (DEFAULT != null) {
      throw new IllegalStateException();
    }
    DEFAULT = accessor;
  }

  public Accessor() {
  }

  public abstract Path getDirectory(Dataset<?> dataset);

  /**
   * @deprecated
   *          will be removed in 0.11.0; use
   *          {@link #getPathIterator(View)}
   */
  @Deprecated
  public abstract void accumulateDatafilePaths(Dataset<?> dataset, Path directory, List<Path> paths) throws IOException;

  /**
   * Returns an {@code Iterable} that will list all of the data files in the
   * given {@link View}.
   * @param view a {@code View}
   * @return An Iterable of all data file paths in the given {@code View}
   *
   * @since 0.9.0
   */
  public abstract Iterable<Path> getPathIterator(View view);

  public abstract void ensureExists(DatasetDescriptor descriptor, Configuration conf);
}
