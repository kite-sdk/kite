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

package org.kitesdk.data.spi;

import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroSchemaConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;

public class Schemas {
  // used to match resource:schema.avsc URIs
  private static final String RESOURCE_URI_SCHEME = "resource";

  public static Schema fromAvsc(InputStream in) throws IOException {
    // the parser has state, so use a new one each time
    return new Schema.Parser().parse(in);
  }

  public static Schema fromAvsc(File location) throws IOException {
    return fromAvsc(
        FileSystem.getLocal(DefaultConfiguration.get()),
        new Path(location.getPath()));
  }

  public static Schema fromAvsc(FileSystem fs, Path path) throws IOException {
    InputStream in = null;
    boolean threw = true;

    try {
      in = fs.open(path);
      Schema schema = new Schema.Parser().parse(in);
      threw = false;
      return schema;
    } finally {
      Closeables.close(in, threw);
    }
  }

  public static Schema fromAvsc(Configuration conf, URI location)
      throws IOException {
    InputStream in = null;
    boolean threw = true;

    try {
      in = open(conf, location);
      Schema schema = fromAvsc(in);
      threw = false;
      return schema;
    } finally {
      Closeables.close(in, threw);
    }
  }

  public static Schema fromAvro(InputStream in) throws IOException {
    GenericDatumReader<GenericRecord> datumReader =
        new GenericDatumReader<GenericRecord>();
    DataFileStream<GenericRecord> stream = null;
    boolean threw = true;

    try {
      stream = new DataFileStream<GenericRecord>(in, datumReader);
      Schema schema = stream.getSchema();
      threw = false;
      return schema;
    } finally {
      Closeables.close(stream, threw);
    }
  }

  public static Schema fromAvro(File location) throws IOException {
    return fromAvro(
        FileSystem.getLocal(DefaultConfiguration.get()),
        new Path(location.getPath()));
  }

  public static Schema fromAvro(FileSystem fs, Path location)
      throws IOException {
    InputStream in = null;
    boolean threw = true;

    try {
      in = fs.open(location);
      Schema schema = fromAvro(in);
      threw = false;
      return schema;
    } finally {
      Closeables.close(in, threw);
    }
  }

  public static Schema fromAvro(Configuration conf, URI location)
      throws IOException {
    InputStream in = null;
    boolean threw = true;

    try {
      in = open(conf, location);
      Schema schema = fromAvro(in);
      threw = false;
      return schema;
    } finally {
      Closeables.close(in, threw);
    }
  }

  public static Schema fromParquet(File location) throws IOException {
    return fromParquet(
        FileSystem.getLocal(DefaultConfiguration.get()),
        new Path(location.getPath()));
  }

  public static Schema fromParquet(FileSystem fs, Path location) throws IOException {
    ParquetMetadata footer = ParquetFileReader.readFooter(fs.getConf(), location);

    String schemaString = footer.getFileMetaData()
        .getKeyValueMetaData().get("parquet.avro.schema");
    if (schemaString == null) {
      // try the older property
      schemaString = footer.getFileMetaData()
          .getKeyValueMetaData().get("avro.schema");
    }

    if (schemaString != null) {
      return new Schema.Parser().parse(schemaString);
    } else {
      return new AvroSchemaConverter()
          .convert(footer.getFileMetaData().getSchema());
    }
  }

  public static Schema fromParquet(Configuration conf, URI location)
      throws IOException {
    Path path = new Path(location);
    return fromParquet(path.getFileSystem(conf), path);
  }

  public static Schema fromJSON(String name, InputStream in) throws IOException {
    return JsonUtil.inferSchema(in, name, 20);
  }

  public static Schema fromJSON(String name, File location) throws IOException {
    return fromJSON(name,
        FileSystem.getLocal(DefaultConfiguration.get()),
        new Path(location.getPath()));
  }

  public static Schema fromJSON(String name, FileSystem fs, Path location)
      throws IOException {
    InputStream in = null;
    boolean threw = true;

    try {
      in = fs.open(location);
      Schema schema = fromJSON(name, in);
      threw = false;
      return schema;
    } finally {
      Closeables.close(in, threw);
    }
  }

  public static Schema fromJSON(String name, Configuration conf, URI location)
      throws IOException {
    InputStream in = null;
    boolean threw = true;

    try {
      in = open(conf, location);
      Schema schema = fromJSON(name, in);
      threw = false;
      return schema;
    } finally {
      Closeables.close(in, threw);
    }
  }

  private static InputStream open(Configuration conf, URI location)
      throws IOException {
    if (RESOURCE_URI_SCHEME.equals(location.getScheme())) {
      return Resources.getResource(
          location.getRawSchemeSpecificPart()).openStream();
    } else {
      Path path = new Path(location);
      return path.getFileSystem(conf).open(path);
    }
  }

}
