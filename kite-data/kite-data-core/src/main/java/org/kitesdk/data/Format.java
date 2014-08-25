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
package org.kitesdk.data;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import javax.annotation.concurrent.Immutable;

/**
 * <p>
 * The data format used for encoding the data in a {@link Dataset}.
 * </p>
 * <p>
 * There are a small number of formats provided. The default is
 * {@link Formats#AVRO}, which is used when you do not explicitly configure a
 * format.
 * </p>
 *
 * @since 0.2.0
 */
@Immutable
public class Format {
  private final String name;
  private final CompressionType defaultCompressionType;
  private final Set<CompressionType> supportedCompressionTypes;

  Format(String name, CompressionType defaultCompressionType,
      CompressionType[] supportedCompressionTypes) {
    this.name = name;
    this.defaultCompressionType = defaultCompressionType;
    this.supportedCompressionTypes = ImmutableSet.copyOf(supportedCompressionTypes);
  }

  /**
   * Get the format's name.
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Get the extension for use in filenames. The extension does not include a
   * dot.
   * @return the extension
   */
  public String getExtension() {
    return name;
  }

  /**
   * Get the {@link CompressionType}s supported by this {@code Format}.
   *
   * @return the supported compression types
   *
   * @since 0.17.0
   */
  public Set<CompressionType> getSupportedCompressionTypes() {
    return supportedCompressionTypes;
  }

  /**
   * Get the default {@link CompressionType} supported by this {@code Format}.
   *
   * @return the default compression type
   *
   * @since 0.17.0
   */
  public CompressionType getDefaultCompressionType() {
    return defaultCompressionType;
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

    return Objects.equal(this.name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name).toString();
  }
}
