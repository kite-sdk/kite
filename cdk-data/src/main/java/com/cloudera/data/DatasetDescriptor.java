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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.io.Closer;
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
  private final PartitionStrategy partitionStrategy;

  /**
   * Create an instance of this class with the supplied {@link Schema} and
   * optional {@link PartitionStrategy}.
   */
  public DatasetDescriptor(Schema schema,
    @Nullable PartitionStrategy partitionStrategy) {

    this.schema = schema;
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
   * associated {@link PartitionStrategy}, false otherwise.
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
    private PartitionStrategy partitionStrategy;

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
     * Configure the dataset's schema from a {@link URL}. A schema is required,
     * and may be set using one of the <code>schema</code> or
     * <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(URL url) throws IOException {
      InputStream in = null;
      Closer closer = Closer.create();

      try {
        in = closer.register(url.openStream());
        return schema(in);
      } finally {
        closer.close();
      }
    }

    /**
     * Configure the dataset's schema from a {@link String}. A schema is
     * required, and may be set using one of the <code>schema</code> or
     * <code>schemaFromAvroDataFile</code> methods.
     *
     * @return An instance of the builder for method chaining.
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
      Closer closer = Closer.create();

      try {
        reader = closer.register(new DataFileReader<GenericRecord>(file, datumReader));
        this.schema = reader.getSchema();
      } finally {
        closer.close();
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
      Closer closer = Closer.create();

      try {
        stream = closer.register(new DataFileStream<GenericRecord>(in, datumReader));
        this.schema = stream.getSchema();
      } finally {
        closer.close();
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
    public Builder schemaFromAvroDataFile(URL url) throws IOException {
      InputStream in = null;
      Closer closer = Closer.create();

      try {
        in = closer.register(url.openStream());
        return schemaFromAvroDataFile(in);
      } finally {
        closer.close();
      }
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

      return new DatasetDescriptor(schema, partitionStrategy);
    }

  }

}
