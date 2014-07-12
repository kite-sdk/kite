/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.predicates;

import com.google.common.base.Objects;
import javax.annotation.Nullable;
import org.apache.avro.Schema;

public class Exists<T> extends RegisteredPredicate<T> {
  public static final Exists INSTANCE = new Exists();

  static {
    RegisteredPredicate.register("exists", new RegisteredPredicate.Factory() {
      @Override
      @SuppressWarnings("unchecked")
      public <T> RegisteredPredicate<T> fromString(String empty, Schema _) {
        return INSTANCE;
      }
    });
  }

  Exists() {
  }

  @Override
  public boolean apply(@Nullable T value) {
    return (value != null);
  }

  @Override
  public String getName() {
    return "exists";
  }

  @Override
  public String toString(Schema _) {
    return "";
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).toString();
  }
}
