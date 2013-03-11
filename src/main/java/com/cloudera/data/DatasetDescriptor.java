package com.cloudera.data;

import javax.annotation.Nullable;

import org.apache.avro.Schema;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

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
public class DatasetDescriptor {

  private Schema schema;
  private PartitionStrategy partitionStrategy;

  /**
   * Create an instance of this class with the supplied {@link Schema} and
   * optional {@link PartitionStrategy}.
   */
  public DatasetDescriptor(Schema schema,
      @Nullable PartitionStrategy partitionStrategy) {

    this.schema = schema;
    this.partitionStrategy = partitionStrategy;
  }

  public Schema getSchema() {
    return schema;
  }

  @Nullable
  public PartitionStrategy getPartitionStrategy() {
    return partitionStrategy;
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
     * Configure the dataset's schema. Required.
     * 
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Configure the dataset's schema from a {@link File}. Required.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(File file) throws IOException {
      this.schema = new Schema.Parser().parse(file);
      return this;
    }

    /**
     * Configure the dataset's schema from an {@link InputStream}. Required.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(InputStream in) throws IOException {
      this.schema = new Schema.Parser().parse(in);
      return this;
    }

    /**
     * Configure the dataset's schema from a {@link URL}. Required.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(URL url) throws IOException {
      InputStream in = null;
      try {
        in = url.openStream();
        return schema(in);
      } finally {
        if (in != null) {
          in.close();
        }
      }
    }

    /**
     * Configure the dataset's schema from a {@link String}. Required.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(String s) throws IOException {
      this.schema = new Schema.Parser().parse(s);
      return this;
    }

    /**
     * Configure the dataset's schema by using the schema from an existing Avro data
     * file.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schemaFromAvroDataFile(File file) throws IOException {
      GenericDatumReader<GenericRecord> datumReader =
          new GenericDatumReader<GenericRecord>();
      DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(file,
          datumReader);
      this.schema = reader.getSchema();
      return this;
    }

    /**
     * Configure the dataset's schema by using the schema from an existing Avro data
     * file.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schemaFromAvroDataFile(InputStream in) throws IOException {
      GenericDatumReader<GenericRecord> datumReader =
          new GenericDatumReader<GenericRecord>();
      DataFileStream<GenericRecord> stream = new DataFileStream<GenericRecord>(in,
          datumReader);
      this.schema = stream.getSchema();
      return this;
    }

    /**
     * Configure the dataset's schema by using the schema from an existing Avro data
     * file.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schemaFromAvroDataFile(URL url) throws IOException {
      InputStream in = null;
      try {
        in = url.openStream();
        return schemaFromAvroDataFile(in);
      } finally {
        if (in != null) {
          in.close();
        }
      }
    }

    /**
     * Configure the dataset's partitioning strategy. Optional.
     * 
     * @return An instance of the builder for method chaining.
     */
    public Builder partitionStrategy(@Nullable PartitionStrategy partitionStrategy) {
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
