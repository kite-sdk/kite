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

import static org.kitesdk.data.CompressionType.*;

/**
 * <p>
 * Contains constant definitions for the standard {@link Format} instances
 * supported by the library. {@link #AVRO} is the default format.
 * </p>
 *
 * @since 0.2.0
 */
public class Formats {

  private Formats() {
  }

  /**
   * AVRO: the
   * <a href="http://avro.apache.org/docs/current/spec.html#Object+Container+Files">
   * Avro row-oriented format</a>
   */
  public static final Format AVRO = new Format("avro", Snappy,
      new CompressionType[] { Uncompressed, Snappy, Deflate, Bzip2 });

  /**
   * PARQUET: the <a href="http://parquet.io/">Parquet columnar format</a>
   */
  public static final Format PARQUET = new Format("parquet", Snappy,
      new CompressionType[] { Uncompressed, Snappy, Deflate });

  /**
   * JSON: white-space separated json values (read-only).
   *
   * @since 0.18.0
   */
  public static final Format JSON = new Format("json", Uncompressed,
      new CompressionType[] { Uncompressed });

  /**
   * CSV: comma-separated values (read-only).
   *
   * @since 0.9.0
   */
  public static final Format CSV = new Format("csv", Uncompressed,
      new CompressionType[] { Uncompressed });

  /**
   * INPUTFORMAT: a mapreduce InputFormat (read-only).
   *
   * @since 0.18.0
   */
  public static final Format INPUTFORMAT = new Format("inputformat", Uncompressed,
      new CompressionType[] { Uncompressed });

  /**
   * Return a {@link Format} for the format name specified. If {@code formatName}
   * is not a valid name, an IllegalArgumentException is thrown. Currently the
   * formats <q>avro</q>, <q>csv</q>, and <q>parquet</q> are supported. Format names are
   * case sensitive.
   *
   * @since 0.9.0
   * @return an appropriate instance of Format
   * @throws IllegalArgumentException if {@code formatName} is not a valid format.
   */
  public static Format fromString(String formatName) {
    if (formatName.equals("avro")) {
      return AVRO;
    } else if (formatName.equals("parquet")) {
      return PARQUET;
    } else if (formatName.equals("json")) {
      return JSON;
    } else if (formatName.equals("csv")) {
      return CSV;
    } else if (formatName.equals("inputformat")) {
      return INPUTFORMAT;
    } else {
      throw new IllegalArgumentException("Unknown format type: " + formatName);
    }
  }

}
