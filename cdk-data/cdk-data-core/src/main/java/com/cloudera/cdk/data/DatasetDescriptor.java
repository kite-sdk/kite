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
package com.cloudera.cdk.data;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import java.net.MalformedURLException;
import java.net.URI;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.apache.avro.reflect.ReflectData;

/**
 * <p>
 * The structural definition of a {@link Dataset}.
 * </p>
 * <p>
 * Each {@code Dataset} has an associated {@link Schema} and optional
 * {@link PartitionStrategy} defined at the time of creation. Instances of this
 * class are used to hold this information. Users are strongly encouraged to use
 * the inner {@link Builder} to create new instances.
 * </p>
 */
@Immutable
public class DatasetDescriptor {

  private final Schema schema;
  private final URL schemaUrl;
  private final Format format;
  private final PartitionStrategy partitionStrategy;

  /**
   * Create an instance of this class with the supplied {@link Schema},
   * and optional {@link PartitionStrategy}. The default {@link Format},
   * {@link Formats#AVRO}, will be used.
   */
  public DatasetDescriptor(Schema schema, @Nullable PartitionStrategy
      partitionStrategy) {

    this(schema, null, Formats.AVRO, partitionStrategy);
  }

  /**
   * Create an instance of this class with the supplied {@link Schema}, optional URL,
   * {@link Format} and optional {@link PartitionStrategy}.
   */
  DatasetDescriptor(Schema schema, @Nullable URL schemaUrl, Format format,
      @Nullable PartitionStrategy partitionStrategy) {

    this.schema = schema;
    this.schemaUrl = schemaUrl;
    this.format = format;
    this.partitionStrategy = partitionStrategy;
  }

  /**
   * Get the associated {@link Schema}. Depending on the underlying storage
   * system, this schema may be simple (i.e. records made up of only scalar
   * types) or complex (i.e. containing other records, lists, and so on).
   * Validation of the supported schemas is performed by the managing
   * repository, not the dataset or descriptor itself.
   *
   * @return the schema
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Get a URL from which the {@link Schema} may be retrieved. Optional. This method
   * may return <code>null</code> if the schema is not stored at a persistent URL,
   * e.g. if it was constructed from a literal string.
   *
   * @return a URL from which the schema may be retrieved
   * @since 0.3.0
   */
  @Nullable
  public URL getSchemaUrl() {
    return schemaUrl;
  }

  /**
   * Get the associated {@link Format} that the data is stored in.
   *
   * @return the format
   * @since 0.2.0
   */
  public Format getFormat() {
    return format;
  }

  /**
   * Get the {@link PartitionStrategy}, if this dataset is partitioned. Calling
   * this method on a non-partitioned dataset is an error. Instead, use the
   * {@link #isPartitioned()} method prior to invocation.
   */
  public PartitionStrategy getPartitionStrategy() {
    Preconditions
      .checkState(
        isPartitioned(),
        "Attempt to retrieve the partition strategy on a non-partitioned descriptor:%s",
        this);

    return partitionStrategy;
  }

  /**
   * Returns true if an associated dataset is partitioned (that is, has an
   * associated {@link PartitionStrategy}), false otherwise.
   */
  public boolean isPartitioned() {
    return partitionStrategy != null;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("schema", schema)
      .add("partitionStrategy", partitionStrategy).toString();
  }

  /**
   * A fluent builder to aid in the construction of {@link DatasetDescriptor}s.
   */
  public static class Builder implements Supplier<DatasetDescriptor> {

    private Schema schema;
    private URL schemaUrl;
    private Format format = Formats.AVRO;
    private PartitionStrategy partitionStrategy;

    public Builder() {
    }

    /**
     * Returns a Builder that will produce copies of {@code descriptor}, if it
     * is not modified. This is intended to help callers copy and update
     * descriptors even though they are {@link Immutable}.
     *
     * @param descriptor A {@link DatasetDescriptor} to copy settings from
     * @return A {@code Builder} configured to copy {@code descriptor}
     */
    public Builder(DatasetDescriptor descriptor) {
      this.schema = descriptor.getSchema();
      this.schemaUrl = descriptor.getSchemaUrl();
      this.format = descriptor.getFormat();
      this.partitionStrategy = descriptor.getPartitionStrategy();
    }

    /**
     * Configure the dataset's schema. A schema is required, and may be set
     * using one of the <code>schema</code> or
     * <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Configure the dataset's schema from a {@link File}. A schema is required,
     * and may be set using one of the <code>schema</code> or
     * <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(File file) throws IOException {
      this.schema = new Schema.Parser().parse(file);
      // don't set schema URL since it is a local file not on a DFS
      return this;
    }

    /**
     * Configure the dataset's schema from an {@link InputStream}. It is the
     * caller's responsibility to close the {@link InputStream}. A schema is
     * required, and may be set using one of the <code>schema</code> or
     * <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(InputStream in) throws IOException {
      this.schema = new Schema.Parser().parse(in);
      return this;
    }

    /**
     * Configure the dataset's schema from a {@link URI}. A schema is required,
     * and may be set using one of the <code>schema</code> or
     * <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(URI uri) throws IOException {
      this.schemaUrl = toURL(uri);

      InputStream in = null;
      boolean threw = true;
      try {
        in = schemaUrl.openStream();
        threw = false;
        return schema(in);
      } finally {
        Closeables.close(in, threw);
      }
    }

    private URL toURL(URI uri) throws MalformedURLException {
      try {
        // try with installed URLStreamHandlers first...
        return uri.toURL();
      } catch (MalformedURLException e) {
        // if that fails then try using the Hadoop protocol handler
        return new URL(null, uri.toString(), new HadoopFileSystemURLStreamHandler());
      }
    }

    /**
     * Configure the dataset's schema from a {@link String}. A schema is
     * required, and may be set using one of the <code>schema</code> or
     * <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
     * @since 0.2.0
     */
    public Builder schema(String s) {
      this.schema = new Schema.Parser().parse(s);
      return this;
    }

    /**
     * Configure the dataset's schema via a Java class type. A schema is required,
     * and may be set using one of the <code>schema</code> or
     * <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
     * @since 0.2.0
     */
    public <T> Builder schema(Class<T> type) {
      this.schema = ReflectData.get().getSchema(type);
      return this;
    }

    /**
     * Configure the dataset's schema by using the schema from an existing Avro
     * data file. A schema is required, and may be set using one of the
     * <code>schema</code> or <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schemaFromAvroDataFile(File file) throws IOException {
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
      DataFileReader<GenericRecord> reader = null;
      boolean threw = true;
      try {
        reader = new DataFileReader<GenericRecord>(file, datumReader);
        this.schema = reader.getSchema();
        threw = false;
      } finally {
        Closeables.close(reader, threw);
      }
      return this;
    }

    /**
     * Configure the dataset's schema by using the schema from an existing Avro
     * data file. It is the caller's responsibility to close the
     * {@link InputStream}. A schema is required, and may be set using one of
     * the <code>schema</code> or <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schemaFromAvroDataFile(InputStream in) throws IOException {
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
      DataFileStream<GenericRecord> stream = null;
      boolean threw = true;
      try {
        stream = new DataFileStream<GenericRecord>(in, datumReader);
        this.schema = stream.getSchema();
        threw = false;
      } finally {
        Closeables.close(stream, threw);
      }
      return this;
    }

    /**
     * Configure the dataset's schema by using the schema from an existing Avro
     * data file. A schema is required, and may be set using one of the
     * <code>schema</code> or <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schemaFromAvroDataFile(URI uri) throws IOException {
      InputStream in = null;
      boolean threw = true;
      try {
        in = toURL(uri).openStream();
        threw = false;
        return schemaFromAvroDataFile(in);
      } finally {
        Closeables.close(in, threw);
      }
    }

    /**
     * Configure the dataset's format. Optional. If not specified {@link Formats#AVRO}
     * is used by default.
     *
     * @return An instance of the builder for method chaining.
     * @since 0.2.0
     */
    public Builder format(Format format) {
      this.format = format;
      return this;
    }

    /**
     * Configure the dataset's partitioning strategy. Optional.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder partitionStrategy(
      @Nullable PartitionStrategy partitionStrategy) {
      this.partitionStrategy = partitionStrategy;
      return this;
    }

    /**
     * Get an instance of the configured dataset descriptor. Subsequent calls
     * will produce new instances that are similarly configure.
     */
    @Override
    public DatasetDescriptor get() {
      Preconditions.checkState(schema != null,
        "Descriptor schema may not be null");

      return new DatasetDescriptor(schema, schemaUrl, format, partitionStrategy);
    }

  }

}
