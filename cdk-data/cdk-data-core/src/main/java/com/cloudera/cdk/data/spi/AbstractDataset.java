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

package com.cloudera.cdk.data.spi;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetAccessor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.Marker;
import com.cloudera.cdk.data.OrderedReader;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.View;
import com.google.common.base.Preconditions;

public abstract class AbstractDataset implements Dataset {

  @Override
  public Dataset getDataset() {
    return this;
  }

  @Override
  @Deprecated
  public <E> DatasetReader<E> getReader() {
    return newReader();
  }

  @Override
  public <E> OrderedReader<E> newOrderedReader() {
    // this method is optional, so default to UnsupportedOperationException
    throw new UnsupportedOperationException(
        "This Dataset does not support OrderedReaders");
  }

  @Override
  @Deprecated
  public <E> DatasetWriter<E> getWriter() {
    return newWriter();
  }

  @Override
  public <E> DatasetAccessor<E> newAccessor() {
    // this method is optional, so default to UnsupportedOperationException
    throw new UnsupportedOperationException(
        "This Dataset does not support random access");
  }

  @Override
  public boolean contains(PartitionKey key) {
    // A Dataset contains all PartitionKeys, only sub-views have to check
    return true;
  }

  @Override
  public boolean contains(Marker marker) {
    // A Dataset contains all PartitionKeys contained by any Marker
    return true;
  }

  @Override
  public View union(View view) {
    Preconditions.checkArgument(this.equals(view.getDataset()),
        "Views can only be unioned if they have the same underlying Dataset");
    return this;
  }

  @Override
  public View from(Marker start) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public View fromAfter(Marker start) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public View to(Marker end) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public View toBefore(Marker end) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public View in(Marker partial) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Iterable<View> getCoveringPartitions() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
