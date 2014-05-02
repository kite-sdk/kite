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

import org.apache.hadoop.mapreduce.InputFormat;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.RefinableView;
import javax.annotation.concurrent.Immutable;
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

  private static final Logger logger = LoggerFactory.getLogger(AbstractDataset.class);

  protected abstract RefinableView<E> asRefinableView();

  @Override
  public Dataset<E> getDataset() {
    return this;
  }

  public abstract AbstractRefinableView<E> filter(Constraints c);

  @Override
  public DatasetWriter<E> newWriter() {
    logger.debug("Getting writer to dataset:{}", this);

    return asRefinableView().newWriter();
  }

  @Override
  public DatasetReader<E> newReader() {
    logger.debug("Getting reader for dataset:{}", this);

    return asRefinableView().newReader();
  }

  @Override
  public boolean includes(E entity) {
    return true;
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

  public InputFormat<E, Void> getDelegateInputFormat() {
    throw new UnsupportedOperationException("No delegate input format defined.");
  }

  @Override
  public boolean deleteAll() {
    throw new UnsupportedOperationException(
        "This Dataset does not support bulk deletion");
  }

}
