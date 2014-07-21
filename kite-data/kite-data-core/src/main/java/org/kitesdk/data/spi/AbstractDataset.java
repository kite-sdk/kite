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

import com.google.common.base.Objects;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.RefinableView;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A common Dataset base class to simplify implementations.
 *
 * @param <E> The type of entities stored in this {@code Dataset}.
 * @since 0.9.0
 */
@Immutable
public abstract class AbstractDataset<E> implements Dataset<E>, RefinableView<E> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDataset.class);

  protected abstract RefinableView<E> asRefinableView();
  protected final Class<E> type;

  public AbstractDataset(Class<E> type, Schema schema) {
    this.type = DataModelUtil.resolveType(type, schema);
  }

  @Override
  public Dataset<E> getDataset() {
    return this;
  }

  public abstract AbstractRefinableView<E> filter(Constraints c);

  @Override
  public DatasetWriter<E> newWriter() {
    LOG.debug("Getting writer to dataset:{}", this);

    return asRefinableView().newWriter();
  }

  @Override
  public DatasetReader<E> newReader() {
    LOG.debug("Getting reader for dataset:{}", this);

    return asRefinableView().newReader();
  }

  @Override
  public boolean includes(E entity) {
    return true;
  }

  @Override
  public Class<E> getType() {
    return type;
  }

  @Override
  public RefinableView<E> with(String name, Object... values) {
    return asRefinableView().with(name, values);
  }

  @Override
  public RefinableView<E> from(String name, Comparable value) {
    return asRefinableView().from(name, value);
  }

  @Override
  public RefinableView<E> fromAfter(String name, Comparable value) {
    return asRefinableView().fromAfter(name, value);
  }

  @Override
  public RefinableView<E> to(String name, Comparable value) {
    return asRefinableView().to(name, value);
  }

  @Override
  public RefinableView<E> toBefore(String name, Comparable value) {
    return asRefinableView().toBefore(name, value);
  }

  @Override
  public boolean deleteAll() {
    throw new UnsupportedOperationException(
        "This Dataset does not support bulk deletion");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || !Objects.equal(getClass(), obj.getClass())) {
      return false;
    }
    AbstractDataset other = (AbstractDataset) obj;
    return Objects.equal(getName(), other.getName()) &&
        Objects.equal(getDescriptor(), other.getDescriptor()) &&
        Objects.equal(getType(), other.getType());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getClass(), getName(), getDescriptor(), type);
  }
}
