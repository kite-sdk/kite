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
package org.kitesdk.data.spi;

import java.util.Collection;
import javax.annotation.concurrent.NotThreadSafe;
import org.kitesdk.data.DatasetDescriptor;

/**
 * <p>
 * A service provider interface for metadata system plugins.
 * </p>
 * <p>
 * Implementations of {@link org.kitesdk.data.DatasetRepository} are written against this
 * interface and, by extension, can store dataset metadata information contained
 * in the {@link org.kitesdk.data.DatasetDescriptor} in a system of the user's choosing.
 * </p>
 * <p>
 * Implementations of {@link MetadataProvider} are typically not thread-safe; that is,
 * the behavior when accessing a single instance from multiple threads is undefined.
 * </p>
 */
@NotThreadSafe
public interface MetadataProvider {

  /**
   * Load the dataset descriptor for the dataset {@code name}.
   *
   * @param name The fully qualified name of an existing dataset.
   * @return A dataset descriptor.
   * @throws org.kitesdk.data.DatasetNotFoundException  If there is no descriptor for {@code name}
   * @throws org.kitesdk.data.MetadataProviderException If the dataset doesn't exist or the descriptor can not be loaded.
   */
  DatasetDescriptor load(String name);

  /**
   * Create a {@code DatasetDescriptor} for the dataset named {@code name}.
   *
   * The descriptor will be stored for the named data set in this provider's
   * metadata store. It is illegal to create more than one descriptor for a
   * named data set, and an exception will be thrown.
   *
   * @param name       The fully qualified name of a dataset.
   * @param descriptor A dataset descriptor.
   * @return The descriptor as persisted to the Metadata store.
   * @throws org.kitesdk.data.DatasetExistsException     If a {@code DatasetDescriptor} already
   *                                    exists for {@code name}
   * @throws org.kitesdk.data.MetadataProviderException  If the {@code DatasetDescriptor} can not
   *                                    be saved
   *
   * @since 0.7.0
   */
  DatasetDescriptor create(String name, DatasetDescriptor descriptor);

  /**
   * Update a {@code DatasetDescriptor} for the dataset named {@code name}.
   *
   * The new descriptor will be stored for the named data set in this provider's
   * metadata store, replacing the previous descriptor. The named dataset must
   * already exist or an error will be thrown.
   *
   * This method is optional.
   *
   * @param name       The fully qualified name of a dataset.
   * @param descriptor A dataset descriptor.
   * @return The descriptor as persisted to the Metadata store.
   *
   * @throws org.kitesdk.data.DatasetNotFoundException      If there is no descriptor for
   *                                       {@code name}
   * @throws UnsupportedOperationException If descriptor updates are not
   *                                       supported by the implementation.
   * @throws org.kitesdk.data.MetadataProviderException     If the dataset descriptor can not be
   *                                       updated.
   *
   * @since 0.7.0
   */
  DatasetDescriptor update(String name, DatasetDescriptor descriptor);

  /**
   * Delete all metadata associated with the dataset named {@code name}.
   *
   * After this method is called, there will be no {@code DatasetDescriptor}
   * stored for the given {@code name}, unless an exception has been thrown. If
   * metadata was removed, this method will return {@code true}. If there was
   * no {@code DatasetDescriptor} corresponding to the given name, then this
   * method will not make any changes and will return {@code false}.
   *
   * @param name The fully qualified name of a dataset.
   * @return {@code true} if the metadata is successfully deleted,
   *         {@code false} if no action was taken.
   * @throws org.kitesdk.data.MetadataProviderException If the dataset metadata exists but can
   *                                   not be deleted.
   */
  boolean delete(String name);

  /**
   * Checks if there is a {@link DatasetDescriptor} for the dataset named
   * {@code name}.
   *
   * @param name a {@code Dataset} name to check the existence of
   * @return true if {@code name} exists, false otherwise
   * @throws org.kitesdk.data.MetadataProviderException
   *
   * @since 0.7.0
   */
  boolean exists(String name);


  /**
   * List the names of the {@link org.kitesdk.data.Dataset}s managed by this
   * {@code MetadataProvider}. If there is not at least one {@code Dataset}, an
   * empty list will be returned.
   *
   * @return a {@link Collection} of Dataset names ({@link String}s)
   * @throws org.kitesdk.data.MetadataProviderException
   *
   * @since 0.8.0
   */
  Collection<String> list();
}
