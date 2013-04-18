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
package com.cloudera.data;

import com.google.common.base.Objects;
import javax.annotation.concurrent.Immutable;

/**
 * <p>
 * The data format used for encoding the data in a {@link Dataset} when stored in a
 * {@link DatasetRepository}.
 * </p>
 * <p>
 * There are a small number of formats provided, the default is {@link Formats#AVRO},
 * which will be used when no format is explicitly configured.
 * </p>
 */
@Immutable
public class Format {
  private final String name;

  Format(String name) {
    this.name = name;
  }

  /**
   * Get the format's name.
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Get the extension for use in filenames. A dot is not included as a part of the
   * extension.
   * @return the extension
   */
  public String getExtension() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }

    Format that = (Format) o;

    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name).toString();
  }
}
