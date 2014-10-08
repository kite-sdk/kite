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
package org.kitesdk.data;

import java.net.URI;
import org.apache.avro.Schema;

import javax.annotation.concurrent.Immutable;

/**
 * <p>
 * A logical representation of a set of data entities.
 * </p>
 * <p>
 * Logically, all datasets have two generic properties: a name, and a descriptor
 * that holds information such as the dataset's schema and its partitioning
 * information. Concrete implementations of {@code Dataset} can support
 * additional properties, mandatory or otherwise, as needed. {@code Dataset}s
 * are not normally instantiated directly, but managed by a repository (also
 * implementation-specific).
 * </p>
 * <p>
 * Implementations of {@link Dataset} are immutable.
 * </p>
 *
 * @param <E> The type of entities stored in this {@code Dataset}.
 *
 * @see View
 * @see DatasetWriter
 * @see DatasetReader
 * @see PartitionStrategy
 * @see DatasetDescriptor
 * @see Schema
 */
@Immutable
public interface Dataset<E> extends RefinableView<E> {

  /**
   * Get the name of a {@code Dataset}. No guarantees are made about the format
   * of this name.
   */
  String getName();

  /**
   * Get the namespace that contains this {@code Dataset}. A namespace is a
   * string that identifies a logical group of datasets.
   *
   * @since 0.17.0
   */
  String getNamespace();

  /**
   * Get the {@link DatasetDescriptor} associated with this dataset.
   */
  DatasetDescriptor getDescriptor();

  /**
   * Return a {@code URI} for this {@code Dataset}.
   *
   * The returned URI should load a copy of this dataset when passed to
   * {@link Datasets#load(java.net.URI, java.lang.Class)}.
   *
   * @return a URI that identifies this dataset
   * @since 0.15.0
   */
  URI getUri();

}
