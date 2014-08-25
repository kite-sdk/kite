/*
 * Copyright 2014 joey.
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

package org.kitesdk.data;

import com.google.common.base.Objects;

/**
 * <p>
 * Contains constant definitions for the standard compression types we support.
 * Not every {@link Format} supports every compression type. Use
 * {@link Format#getSupportedCompressionTypes()} to see what compression
 * types your {@link Format} supports.
 * </p>
 * @since 0.17.0
 */
public enum CompressionType {

  Snappy("snappy"),
  Deflate("deflate"),
  Bzip2("bzip2"),
  Lzo("lzo"),
  Uncompressed("uncompressed");

  private final String name;

  private CompressionType(String name) {
    this.name = name;
  }

  /**
   * Get the {@code String} name for this compression type. This name can be
   * passed to {@link #forName(java.lang.String)} to return this instance.
   *
   * @return the name of the compression type
   */
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name).toString();
  }

  /**
   * Return a {@code CompressionType} for the compression type name specified.
   * If {@code name} is not a valid name, an
   * {@link IllegalArgumentException} is thrown. Current the compression types
   * <q>snappy</q>, <q>deflate</q>, <q>bzip2</q>, and <q>lzo</q> are supported.
   * Not all compression types are supported by all {@link Format}s.
   *
   * @param name the name of the compression type
   * @return the appropriate CompressionType
   *
   * @throws IllegalArgumentException if {@code name} is not a valid compression type.
   */
  public static CompressionType forName(String name) {
    for (CompressionType type : values()) {
      if (type.name.equals(name)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown compression type: " + name);
  }

}
