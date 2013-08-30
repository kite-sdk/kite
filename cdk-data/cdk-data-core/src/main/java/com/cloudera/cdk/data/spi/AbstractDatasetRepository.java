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
import com.cloudera.cdk.data.DatasetRepository;

/**
 * A common DatasetRepository base class to simplify implementation.
 *
 * Implementers can use this class to maintain compatibility with old API calls,
 * without needing to implement deprecated methods. It also includes backwards-
 * compatible implementations of current API methods so that implementers don't
 * need to implement deprecated methods.
 */
public abstract class AbstractDatasetRepository implements DatasetRepository {

  // for detecting call loops; remove these in CDK 0.8.x
  private boolean inGet = false;
  private boolean inDrop = false;

  @Deprecated
  @Override
  public Dataset get(String name) {
    // this is for API backward-compatibility
    try {
      this.inGet = true;
      return load(name);
    } finally {
      this.inGet = false;
    }
  }

  @Override
  public Dataset load(String name) {
    // this is for implementation backward-compatibility
    if (inGet) {
      throw new UnsupportedOperationException(
          "You must implement DatasetRepository#load");
    }
    return get(name);
  }

  @Deprecated
  @Override
  public boolean drop(String name) {
    // this is for API backward-compatibility
    try {
      this.inDrop = true;
      return delete(name);
    } finally {
      this.inDrop = false;
    }
  }

  @Override
  public boolean delete(String name) {
    // this is for implementation backward-compatibility
    if (inDrop) {
      throw new UnsupportedOperationException(
          "You must implement DatasetRepository#delete");
    }
    return drop(name);
  }

}
