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

import org.kitesdk.data.DatasetReader;
import java.util.Iterator;
import org.kitesdk.data.DatasetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A common DatasetReader base class to simplify implementations.
 *
 * @param <E> The type of entities returned by this DatasetReader.
 */
public abstract class AbstractDatasetReader<E>
    implements DatasetReader<E>, InitializeAccessor {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetReader.class);

  @Override
  @Deprecated
  public void open() {
    LOG.warn("DatasetReader#open is no longer needed and will be removed");
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException(
            "This Dataset does not support remove.");
  }

  @Override
  public Iterator<E> iterator() {
    return this;
  }
}
