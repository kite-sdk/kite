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

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.ColumnMappingParser;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.HadoopFileSystemURLStreamHandler;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.SchemaUtil;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;
import org.kitesdk.data.spi.partition.ProvidedFieldPartitioner;

/**
 * <p>
 * The structural definition of a {@link Dataset}.
 * </p>
 * <p>
 * Each {@code Dataset} has an associated {@link Schema} and optional
 * {@link PartitionStrategy} defined at the time of creation. You use instances
 * of this class to hold this information. You are strongly encouraged to use
 * the inner {@link Builder} to create new instances.
 * </p>
 */
@Immutable
public class DatasetDescriptor {

  private final Schema schema;
  private final URL schemaUrl;
  private final URI schemaUri;
  private final Format format;
  private final URI location;
  private final Map<String, String> properties;
  private final PartitionStrategy partitionStrategy;
  private final ColumnMapping columnMappings;
  private final CompressionType compressionType;

  /**
   * Create an instance of this class with the supplied {@link Schema},
   * optional URL, {@link Format}, optional location URL, and optional
   * {@link PartitionStrategy}.
   */
  public DatasetDescriptor(Schema schema, @Nullable URL schemaUrl, Format format,
      @Nullable URI location, @Nullable Map<String, String> properties,
      @Nullable PartitionStrategy partitionStrategy) {
    this(schema, schemaUrl, format, location, properties, partitionStrategy,
        null);
  }

  /**
   * Create an instance of this class with the supplied {@link Schema}, optional
   * URL, {@link Format}, optional location URL, optional
   * {@link PartitionStrategy}, and optional {@link ColumnMapping}.
   *
   * @since 0.14.0
   */
  public DatasetDescriptor(Schema schema, @Nullable URL schemaUrl,
      Format format, @Nullable URI location,
      @Nullable Map<String, String> properties,
      @Nullable PartitionStrategy partitionStrategy,
      @Nullable ColumnMapping columnMapping) {
    this(schema, toURI(schemaUrl), format, location,
        properties, partitionStrategy, columnMapping, null);
  }

  /**
   * Create an instance of this class with the supplied {@link Schema}, optional
   * URL, {@link Format}, optional location URL, optional
   * {@link PartitionStrategy}, optional {@link ColumnMapping}, and optional
   * {@link CompressionType}.
   *
   * @since 0.17.0
   */
  public DatasetDescriptor(Schema schema, @Nullable URI schemaUri,
      Format format, @Nullable URI location,
      @Nullable Map<String, String> properties,
      @Nullable PartitionStrategy partitionStrategy,
      @Nullable ColumnMapping columnMapping,
      @Nullable CompressionType compressionType) {
    // URI can be null if the descriptor is configuring a new Dataset
    Preconditions.checkArgument(
        (location == null) || (location.getScheme() != null),
        "Location URIs must be fully-qualified and have a FS scheme.");
    checkCompressionType(format, compressionType);

    this.schema = schema;
    this.schemaUri = schemaUri;
    this.schemaUrl = toURL(schemaUri);
    this.format = format;
    this.location = location;
    if (properties != null) {
      this.properties = ImmutableMap.copyOf(properties);
    } else {
      this.properties = ImmutableMap.of();
    }
    this.partitionStrategy = partitionStrategy;
    this.columnMappings = columnMapping;

    // if no compression format is present, get the default from the format
    this.compressionType = compressionType == null ?
        this.format.getDefaultCompressionType() : compressionType;
  }

  /**
   * Get the associated {@link Schema}. Depending on the underlying storage
   * system, this schema can be simple (that is, records made up of only scalar
   * types) or complex (that is, containing other records, lists, and so on).
   * Validation of the supported schemas is performed by the managing
   * repository, not the dataset or descriptor itself.
   *
   * @return the schema
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Get a URL from which the {@link Schema} can be retrieved (optional). This
   * method might return {@code null} if the schema is not stored at a persistent
   * URL (for example, if it were constructed from a literal string).
   *
   * @return a URL from which the schema can be retrieved
   * @since 0.3.0
   */
  @Nullable
  public URL getSchemaUrl() {
    return schemaUrl;
  }

  /**
   * Get the associated {@link Format} the data is stored in.
   *
   * @return the format
   * @since 0.2.0
   */
  public Format getFormat() {
    return format;
  }

  /**
   * Get the URL location where the data for this {@link Dataset} is stored
   * (optional).
   *
   * @return a location URL or null if one is not set
   *
   * @since 0.8.0
   */
  @Nullable
  public URI getLocation() {
    return location;
  }

  /**
   * Get a named property.
   *
   * @param name the String property name to get.
   * @return the String value of the property, or null if it does not exist.
   *
   * @since 0.8.0
   */
  @Nullable
  public String getProperty(String name) {
    return properties.get(name);
  }

  /**
   * Check if a named property exists.
   *
   * @param name the String property name.
   * @return true if the property exists, false otherwise.
   *
   * @since 0.8.0
   */
  public boolean hasProperty(String name) {
    return properties.containsKey(name);
  }

  /**
   * List the names of all custom properties set.
   *
   * @return a Collection of String property names.
   *
   * @since 0.8.0
   */
  public Collection<String> listProperties() {
    return properties.keySet();
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
   * Get the {@link ColumnMapping}.
   *
   * @return ColumnMapping
   *
   * @since 0.14.0
   */
  public ColumnMapping getColumnMapping() {
    return columnMappings;
  }

  /**
   * Get the {@link CompressionType}
   *
   * @return the compression format
   *
   * @since 0.17.0
   */
  public CompressionType getCompressionType() {
    return compressionType;
  }

  /**
   * Returns true if an associated dataset is partitioned (that is, has an
   * associated {@link PartitionStrategy}), false otherwise.
   */
  public boolean isPartitioned() {
    return partitionStrategy != null;
  }

  /**
   * Returns true if an associated dataset is column mapped (that is, has an
   * associated {@link ColumnMapping}), false otherwise.
   *
   * @since 0.14.0
   */
  public boolean isColumnMapped() {
    return columnMappings != null;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schema, format, location, properties,
        partitionStrategy, columnMappings, compressionType);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
      justification="Default annotation is not correct for equals")
  public boolean equals(@Nullable Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final DatasetDescriptor other = (DatasetDescriptor) obj;
    return (
        Objects.equal(schema, other.schema) &&
        Objects.equal(format, other.format) &&
        Objects.equal(location, other.location) &&
        Objects.equal(properties, other.properties) &&
        Objects.equal(partitionStrategy, other.partitionStrategy) &&
        Objects.equal(columnMappings, other.columnMappings) &&
        Objects.equal(compressionType, other.compressionType));
  }

  @Override
  public String toString() {
    Objects.ToStringHelper helper = Objects.toStringHelper(this)
        .add("format", format)
        .add("schema", schema)
        .add("location", location)
        .add("properties", properties)
        .add("partitionStrategy", partitionStrategy)
        .add("compressionType", compressionType);
    if (isColumnMapped()) {
      helper.add("columnMapping", columnMappings);
    }
    return helper.toString();
  }

  /**
   * A fluent builder to aid in the construction of {@link DatasetDescriptor}s.
   */
  public static class Builder {

    // used to match resource:schema.avsc URIs
    private static final String RESOURCE_URI_SCHEME = "resource";

    private Configuration conf;
    private URI defaultFS;

    private Schema schema;
    private URI schemaUri;
    private Format format = Formats.AVRO;
    private URI location;
    private Map<String, String> properties;
    private PartitionStrategy partitionStrategy;
    private ColumnMapping columnMapping;
    private ColumnMapping copiedMapping;
    private CompressionType compressionType;

    public Builder() {
      this.properties = Maps.newHashMap();
      this.conf = DefaultConfiguration.get();
      try {
        this.defaultFS = FileSystem.get(conf).getUri();
      } catch (IOException e) {
        throw new DatasetIOException("Cannot get the default FS", e);
      }
    }

    /**
     * Creates a Builder configured to copy {@code descriptor}, if it is not
     * modified. This is intended to help callers copy and update descriptors
     * even though they are {@link Immutable}.
     *
     * @param descriptor A {@link DatasetDescriptor} to copy settings from
     * @since 0.7.0
     */
    public Builder(DatasetDescriptor descriptor) {
      this();
      this.schema = descriptor.schema;
      this.schemaUri = descriptor.schemaUri;
      this.format = descriptor.format;
      this.location = descriptor.location;
      this.copiedMapping = descriptor.columnMappings;
      this.compressionType = descriptor.compressionType;
      this.partitionStrategy = descriptor.partitionStrategy;
      properties.putAll(descriptor.properties);
    }

    /**
     * Configure the dataset's schema. A schema is required, and can be set
     * using one of the methods: {@code schema}, {@code schemaLiteral},
     * {@code schemaUri}, or {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(Schema schema) {
      Preconditions.checkNotNull(schema, "Schema cannot be null");
      this.schema = schema;
      return this;
    }

    /**
     * Configure the dataset's schema from a {@link File}. A schema is required,
     * and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
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
     * required, and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(InputStream in) throws IOException {
      this.schema = new Schema.Parser().parse(in);
      return this;
    }

    /**
     * Configure the {@link Dataset}'s schema from a URI. A schema is required,
     * and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @param uri a URI object for the schema's location.
     * @return An instance of the builder for method chaining.
     * @throws IOException
     *
     * @since 0.8.0
     */
    public Builder schemaUri(URI uri) throws IOException {
      this.schemaUri = qualifiedUri(uri);

      InputStream in = null;
      boolean threw = true;
      try {
        in = open(uri);
        schema(in);
        threw = false;
      } finally {
        Closeables.close(in, threw);
      }
      return this;
    }

    /**
     * Configure the {@link Dataset}'s schema from a String URI. A schema is
     * required, and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @param uri a String URI
     * @return An instance of the builder for method chaining.
     * @throws IOException
     *
     * @since 0.8.0
     */
    public Builder schemaUri(String uri) throws IOException {
      return schemaUri(URI.create(uri));
    }

    /**
     * Configure the dataset's schema from a {@link String}. A schema is
     * required, and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     *
     * @since 0.8.0
     */
    public Builder schemaLiteral(String s) {
      this.schema = new Schema.Parser().parse(s);
      return this;
    }

    /**
     * Configure the dataset's schema via a Java class type. A schema is
     * required, and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
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
     * data file. A schema is required, and can be set using one of the methods
     * {@code schema}, {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
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
     * {@link InputStream}. A schema is required, and can be set using one of
     * the methods  {@code schema},  {@code schemaLiteral}, {@code schemaUri},
     * or {@code schemaFromAvroDataFile}.
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
     * data file. A schema is required, and can be set using one of the methods
     * {@code schema}, {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schemaFromAvroDataFile(URI uri) throws IOException {
      InputStream in = null;
      boolean threw = true;
      try {
        in = open(uri);
        schemaFromAvroDataFile(in);
        threw = false;
      } finally {
        Closeables.close(in, threw);
      }
      return this;
    }

    /**
     * Configure the dataset's format (optional). If not specified
     * {@link Formats#AVRO} is used by default.
     *
     * @return An instance of the builder for method chaining.
     * @since 0.2.0
     */
    public Builder format(Format format) {
      this.format = format;
      return this;
    }

    /**
     * Configure the dataset's format from a format name String (optional). If
     * not specified, {@link Formats#AVRO} is used by default.
     *
     * @param formatName a String format name
     * @return An instance of the builder for method chaining.
     * @throws UnknownFormatException if the format name is not recognized.
     *
     * @since 0.8.0
     */
    public Builder format(String formatName) {
      return this.format(Formats.fromString(formatName));
    }

    /**
     * Configure the dataset's location (optional).
     *
     * @param uri A URI location
     * @return An instance of the builder for method chaining.
     *
     * @since 0.8.0
     */
    public Builder location(@Nullable URI uri) {
      // URI can be null if the descriptor is configuring a new Dataset
      Preconditions.checkArgument((uri == null) || (uri.getScheme() != null),
          "Location URIs must be fully-qualified and have a FS scheme.");
      this.location = uri;
      return this;
    }

    /**
     * Configure the dataset's location (optional).
     *
     * @param uri A location Path
     * @return An instance of the builder for method chaining.
     *
     * @since 0.8.0
     */
    public Builder location(Path uri) {
      return location(uri.toUri());
    }

    /**
     * Configure the dataset's location (optional).
     *
     * @param uri A location String URI
     * @return An instance of the builder for method chaining.
     *
     * @since 0.8.0
     */
    public Builder location(String uri) {
      return location(URI.create(uri));
    }

    /**
     * Add a key-value property to the descriptor.
     *
     * @param name the property name
     * @param value the property value
     * @return An instance of the builder for method chaining.
     *
     * @since 0.8.0
     */
    public Builder property(String name, String value) {
      this.properties.put(name, value);
      return this;
    }

    /**
     * Configure the dataset's partitioning strategy (optional).
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder partitionStrategy(
        @Nullable PartitionStrategy partitionStrategy) {
      this.partitionStrategy = partitionStrategy;
      return this;
    }

    /**
     * Configure the dataset's partition strategy from a File.
     *
     * The File contents must be a JSON-formatted partition strategy that is
     * produced by {@link PartitionStrategy#toString()}.
     *
     * @param file
     *          The File
     * @return
     *          An instance of the builder for method chaining.
     * @throws ValidationException
     *          If the file does not contain a valid JSON-encoded partition
     *          strategy
     * @throws DatasetIOException
     *          If there is an IOException accessing the file contents
     *
     * @since 0.14.0
     */
    public Builder partitionStrategy(File file) {
      this.partitionStrategy = PartitionStrategyParser.parse(file);
      return this;
    }

    /**
     * Configure the dataset's partition strategy from an InputStream.
     *
     * The InputStream contents must be a JSON-formatted partition strategy
     * that is produced by {@link PartitionStrategy#toString()}.
     *
     * @param in
     *          The input stream
     * @return An instance of the builder for method chaining.
     * @throws ValidationException
     *          If the stream does not contain a valid JSON-encoded partition
     *          strategy
     * @throws DatasetIOException
     *          If there is an IOException accessing the InputStream contents
     *
     * @since 0.14.0
     */
    public Builder partitionStrategy(InputStream in) {
      this.partitionStrategy = PartitionStrategyParser.parse(in);
      return this;
    }

    /**
     * Configure the dataset's partition strategy from a String literal.
     *
     * The String literal is a JSON-formatted partition strategy that can be
     * produced by {@link PartitionStrategy#toString()}.
     *
     * @param literal
     *          A partition strategy String literal
     * @return This builder for method chaining.
     * @throws ValidationException
     *          If the literal is not a valid JSON-encoded partition strategy
     *
     * @since 0.14.0
     */
    public Builder partitionStrategyLiteral(String literal) {
      this.partitionStrategy = PartitionStrategyParser.parse(literal);
      return this;
    }

    /**
     * Configure the dataset's partition strategy from a URI.
     *
     * @param uri
     *          A URI to a partition strategy JSON file.
     * @return This builder for method chaining.
     * @throws ValidationException
     *          If the literal is not a valid JSON-encoded partition strategy
     *
     * @since 0.14.0
     */
    public Builder partitionStrategyUri(URI uri) throws IOException {
      InputStream in = null;
      boolean threw = true;
      try {
        in = open(uri);
        partitionStrategy(in);
        threw = false;
      } finally {
        Closeables.close(in, threw);
      }
      return this;
    }

    /**
     * Configure the dataset's partition strategy from a String URI.
     *
     * @param uri
     *          A String URI to a partition strategy JSON file.
     * @return This builder for method chaining.
     * @throws ValidationException
     *          If the literal is not a valid JSON-encoded partition strategy
     *
     * @since 0.14.0
     */
    public Builder partitionStrategyUri(String uri) throws IOException {
      return partitionStrategyUri(URI.create(uri));
    }

    /**
     * Configure the dataset's column mapping descriptor (optional)
     *
     * @param columnMappings
     *          A ColumnMapping
     * @return This builder for method chaining
     *
     * @since 0.14.0
     */
    public Builder columnMapping(
        @Nullable ColumnMapping columnMappings) {
      this.columnMapping = columnMappings;
      return this;
    }

    /**
     * Configure the dataset's column mapping descriptor from a File.
     *
     * The File contents must be a JSON-formatted column mapping. This format
     * can produced by {@link ColumnMapping#toString()}.
     *
     * @param file
     *          The file
     * @return This builder for method chaining
     * @throws ValidationException
     *          If the literal is not valid JSON-encoded column mappings
     * @throws DatasetIOException
     *          If there is an IOException accessing the file contents
     *
     * @since 0.14.0
     */
    public Builder columnMapping(File file) {
      this.columnMapping = ColumnMappingParser.parse(file);
      return this;
    }

    /**
     * Configure the dataset's column mapping descriptor from an InputStream.
     *
     * The InputStream contents must be a JSON-formatted column mapping. This
     * format can produced by {@link ColumnMapping#toString()}.
     *
     * @param in
     *          The input stream
     * @return This builder for method chaining
     * @throws ValidationException
     *          If the literal is not valid JSON-encoded column mappings
     * @throws DatasetIOException
     *          If there is an IOException accessing the InputStream contents
     *
     * @since 0.14.0
     */
    public Builder columnMapping(InputStream in) {
      this.columnMapping = ColumnMappingParser.parse(in);
      return this;
    }

    /**
     * Configure the dataset's column mappings from a String literal.
     *
     * The String literal is a JSON-formatted representation that can be
     * produced by {@link ColumnMapping#toString()}.
     *
     * @param literal
     *          A column mapping String literal
     * @return This builder for method chaining
     * @throws ValidationException
     *          If the literal is not valid JSON-encoded column mappings
     *
     * @since 0.14.0
     */
    public Builder columnMappingLiteral(String literal) {
      this.columnMapping = ColumnMappingParser.parse(literal);
      return this;
    }

    /**
     * Configure the dataset's column mappings from a URI.
     *
     * @param uri
     *          A URI to a column mapping JSON file
     * @return This builder for method chaining
     * @throws ValidationException
     *          If the literal is not valid JSON-encoded column mappings
     * @throws java.io.IOException
     *          If accessing the URI results in an IOException
     *
     * @since 0.14.0
     */
    public Builder columnMappingUri(URI uri) throws IOException {
      InputStream in = null;
      boolean threw = true;
      try {
        in = open(uri);
        columnMapping(in);
        threw = false;
      } finally {
        Closeables.close(in, threw);
      }
      return this;
    }

    /**
     * Configure the dataset's column mappings from a String URI.
     *
     * @param uri
     *          A String URI to a column mapping JSON file
     * @return This builder for method chaining
     * @throws ValidationException
     *          If the literal is not valid JSON-encoded column mappings
     * @throws java.io.IOException
     *          If accessing the URI results in an IOException
     *
     * @since 0.14.0
     */
    public Builder columnMappingUri(String uri) throws IOException {
      return columnMappingUri(URI.create(uri));
    }

    /**
     * Configure the dataset's compression format (optional). If not set,
     * default to {@link CompressionType#Snappy}.
     *
     * @param compressionType the compression format
     *
     * @return This builder for method chaining
     *
     * @since 0.17.0
     */
    public Builder compressionType(CompressionType compressionType) {
      this.compressionType = compressionType;
      return this;
    }

    /**
     * Configure the dataset's compression format (optional). If not set,
     * default to {@link CompressionType#Snappy}.
     *
     * @param compressionTypeName  the name of the compression format
     *
     * @return This builder for method chaining
     *
     * @since 0.17.0
     */
    public Builder compressionType(String compressionTypeName) {
      return compressionType(CompressionType.forName(compressionTypeName));
    }

    /**
     * Build an instance of the configured dataset descriptor. Subsequent calls
     * produce new instances that are similarly configured.
     *
     * @since 0.9.0
     */
    public DatasetDescriptor build() {
      ValidationException.check(schema != null,
          "Descriptor schema is required and cannot be null");

      // if no partition strategy is defined, check for one in the schema
      if (partitionStrategy == null) {
        if (PartitionStrategyParser.hasEmbeddedStrategy(schema)) {
          this.partitionStrategy = PartitionStrategyParser.parseFromSchema(schema);
        }
      }

      // if no column mappings are present, check for them in the schema
      if (columnMapping == null) {
        if (ColumnMappingParser.hasEmbeddedColumnMapping(schema)) {
          this.columnMapping = ColumnMappingParser.parseFromSchema(schema);
        } else if (ColumnMappingParser.hasEmbeddedFieldMappings(schema)) {
          this.columnMapping = ColumnMappingParser.parseFromSchemaFields(schema);
          if (partitionStrategy == null) {
            // For backward-compatibility, build a strategy from key values
            this.partitionStrategy = buildPartitionStrategyForKeyMappings(
                ColumnMappingParser.parseKeyMappingsFromSchemaFields(schema));
          }
        }
      }

      // if we still don't have a column mapping, see if we had one copied from
      // another descriptor
      if (columnMapping == null && copiedMapping != null) {
        columnMapping = copiedMapping;
      }

      checkPartitionStrategy(schema, partitionStrategy);
      checkColumnMappings(schema, partitionStrategy, columnMapping);
      // TODO: verify that all fields have a mapping?

      return new DatasetDescriptor(
          schema, schemaUri, format, location, properties, partitionStrategy,
          columnMapping, compressionType);
    }

    private InputStream open(URI location) throws IOException {
      if (RESOURCE_URI_SCHEME.equals(location.getScheme())) {
        return Resources.getResource(location.getRawSchemeSpecificPart()).openStream();
      } else {
        Path filePath = new Path(qualifiedUri(location));
        // even though it was qualified using the default FS, it may not be in it
        FileSystem fs = filePath.getFileSystem(conf);
        return fs.open(filePath);
      }
    }

    private URI qualifiedUri(URI location) throws IOException {
      if (RESOURCE_URI_SCHEME.equals(location.getScheme())) {
        return null;
      } else {
        boolean useDefault = defaultFS.getScheme().equals(location.getScheme());
        // work around a bug in Path where the authority for a different scheme
        // will be used for the location.
        return new Path(location)
            .makeQualified(useDefault ? defaultFS : location, new Path("/"))
            .toUri();
      }
    }

    private static void checkPartitionStrategy(
        Schema schema, @Nullable PartitionStrategy strategy) {
      if (strategy == null) {
        return;
      }
      for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
        if (fp instanceof ProvidedFieldPartitioner) {
          // provided partitioners are not based on the entity fields
          continue;
        }

        // check the entity is a record if there is a non-provided partitioner
        ValidationException.check(schema.getType() == Schema.Type.RECORD,
            "Cannot partition non-records: %s", schema);

        // the source name should be a field in the schema, but not necessarily
        // the record.
        Schema fieldSchema;
        try {
          fieldSchema = SchemaUtil.fieldSchema(schema, fp.getSourceName());
        } catch (IllegalArgumentException e) {
          throw new ValidationException(
              "Cannot partition on " + fp.getSourceName(), e);
        }
        ValidationException.check(
            SchemaUtil.isConsistentWithExpectedType(
                fieldSchema.getType(), fp.getSourceType()),
            "Field type %s does not match partitioner %s",
            fieldSchema.getType(), fp);
      }
    }
  }

  private static void checkCompressionType(Format format,
      @Nullable CompressionType compressionType) {

    if (compressionType == null) {
      return;
    }

    ValidationException.check(format.getSupportedCompressionTypes()
            .contains(compressionType),
        "Format %s doesn't support compression format %s", format.getName(),
        compressionType.getName());
  }

  private static void checkColumnMappings(Schema schema,
                                          @Nullable PartitionStrategy strategy,
                                          @Nullable ColumnMapping mappings) {
    if (mappings == null) {
      return;
    }
    ValidationException.check(schema.getType() == Schema.Type.RECORD,
        "Cannot map non-records: %s", schema);
    Set<String> keyMappedFields = Sets.newHashSet();
    for (FieldMapping fm : mappings.getFieldMappings()) {
      Schema fieldSchema = SchemaUtil.fieldSchema(schema, fm.getFieldName());
      ValidationException.check(
          SchemaUtil.isConsistentWithMappingType(
              fieldSchema.getType(), fm.getMappingType()),
          "Field type %s is not compatible with mapping %s",
          fieldSchema.getType(), fm);
      if (FieldMapping.MappingType.KEY == fm.getMappingType()) {
        keyMappedFields.add(fm.getFieldName());
      }
    }
    // verify that all key mapped fields have a corresponding id partitioner
    if (strategy != null) {
      for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
        if (fp instanceof IdentityFieldPartitioner) {
          keyMappedFields.remove(fp.getSourceName());
        }
      }
    }
    // any remaining keyMappedFields are invalid
    if (keyMappedFields.size() > 0) {
      throw new ValidationException(
          "Fields are key-mapped without identity partitioners: " +
          Joiner.on(", ").join(keyMappedFields));
    }
  }

  private static PartitionStrategy buildPartitionStrategyForKeyMappings(
      Map<Integer, FieldMapping> keyMappings) {
    PartitionStrategy.Builder builder = new PartitionStrategy.Builder();
    for (Integer index : new TreeSet<Integer>(keyMappings.keySet())) {
      builder.identity(keyMappings.get(index).getFieldName());
    }
    return builder.build();
  }

  private static URI toURI(@Nullable URL url) {
    if (url == null) {
      return null;
    }
    // throw IllegalArgumentException if the URL is not a URI
    return URI.create(url.toExternalForm());
  }

  private static URL toURL(@Nullable URI uri) {
    if (uri == null) {
      return null;
    }
    try {
      // try with installed URLStreamHandlers first...
      return uri.toURL();
    } catch (MalformedURLException e) {
      try {
        // if that fails then try using the Hadoop protocol handler
        return new URL(null, uri.toString(), new HadoopFileSystemURLStreamHandler());
      } catch (MalformedURLException _) {
        return null; // can't produce a URL
      }
    }
  }
}
