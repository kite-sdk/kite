/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;

/**
 * A common DatasetRepository base class to simplify implementation.
 *
 * Implementers can use this class to maintain compatibility with old API calls,
 * without needing to implement deprecated methods. It also includes backwards-
 * compatible implementations of current API methods so that implementers don't
 * need to implement deprecated methods.
 */
@SuppressWarnings("deprecation") // this class helps transition
public abstract class AbstractDatasetRepository implements DatasetRepository {
  @SuppressWarnings("unchecked")
  @Override
  public <E> Dataset<E> create(String namespace, String name, DatasetDescriptor descriptor) {
    return (Dataset<E>) create(namespace, name, descriptor, Object.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <E> Dataset<E> load(String namespace, String name) {
    return (Dataset<E>) load(namespace, name, Object.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <E> Dataset<E> update(String namespace, String name, DatasetDescriptor descriptor) {
    return (Dataset<E>) update(namespace, name, descriptor, Object.class);
  }
}
