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

import com.cloudera.cdk.data.DatasetReader;
import java.util.Iterator;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * A common DatasetReader base class to simplify implementations.
 *
 * @param <E> The type of entities returned by this DatasetReader.
 */
public abstract class AbstractDatasetReader<E> implements DatasetReader<E> {

  // used to detect a next-read call loop, can be removed with #read()
  private boolean inRead = false;

  @Deprecated
  @Override
  public E read() {
    // this is for API backward-compatibility
    try {
      this.inRead = true;
      return next();
    } finally {
      this.inRead = false;
    }
  }

  @SuppressWarnings(value="IT_NO_SUCH_ELEMENT",
      justification="For backward-compatibility with the non-Iterator API.")
  @Override
  public E next() {
    // this is for implementation backward-compatibility
    if (inRead) {
      // the concrete class is using the default implementation of read and
      // next, which is a stack loop.
      throw new UnsupportedOperationException(
              "You must implement DatasetReader#next");
    }
    return read();
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
