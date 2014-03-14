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

import org.kitesdk.data.spi.URIPattern;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import org.kitesdk.data.spi.SchemaUtil;

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
  private final URI location;
  private final Map<String, String> properties;
  private final PartitionStrategy partitionStrategy;

  /**
   * Create an instance of this class with the supplied {@link Schema},
   * optional URL, {@link Format}, optional location URL, and optional
   * {@link PartitionStrategy}.
   */
  public DatasetDescriptor(Schema schema, @Nullable URL schemaUrl, Format format,
      @Nullable URI location, @Nullable Map<String, String> properties,
      @Nullable PartitionStrategy partitionStrategy) {
    // URI can be null if the descriptor is configuring a new Dataset
    Preconditions.checkArgument(
        (location == null) || (location.getScheme() != null),
        "Location URIs must be fully-qualified and have a FS scheme.");

    this.schema = schema;
    this.schemaUrl = schemaUrl;
    this.format = format;
    this.location = location;
    if (properties != null) {
      this.properties = ImmutableMap.copyOf(properties);
    } else {
      this.properties = ImmutableMap.of();
    }
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
   * Get a URL from which the {@link Schema} may be retrieved (optional). This
   * method may return {@code null} if the schema is not stored at a persistent
   * URL, e.g. if it was constructed from a literal string.
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
   * Returns true if an associated dataset is partitioned (that is, has an
   * associated {@link PartitionStrategy}), false otherwise.
   */
  public boolean isPartitioned() {
    return partitionStrategy != null;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        schema, format, location, properties, partitionStrategy);
  }

  @Override
  public boolean equals(Object obj) {
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
        Objects.equal(partitionStrategy, other.partitionStrategy));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("format", format)
        .add("schema", schema)
        .add("location", location)
        .add("properties", properties)
        .add("partitionStrategy", partitionStrategy)
        .toString();
  }

  /**
   * A fluent builder to aid in the construction of {@link DatasetDescriptor}s.
   */
  public static class Builder {

    // used to match resource:schema.avsc URIs
    private static final String RESOURCE_PATH = "resource-path";
    private static final URIPattern RESOURCE_URI_PATTERN =
        new URIPattern(URI.create("resource:*" + RESOURCE_PATH));

    private Schema schema;
    private URL schemaUrl;
    private Format format = Formats.AVRO;
    private URI location;
    private Map<String, String> properties;
    private PartitionStrategy partitionStrategy;

    public Builder() {
      this.properties = Maps.newHashMap();
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
      this.schema = descriptor.getSchema();
      this.schemaUrl = descriptor.getSchemaUrl();
      this.format = descriptor.getFormat();
      this.properties = Maps.newHashMap(descriptor.properties);
      this.location = descriptor.getLocation();

      if (descriptor.isPartitioned()) {
        this.partitionStrategy = descriptor.getPartitionStrategy();
      }
    }

    /**
     * Configure the dataset's schema. A schema is required, and may be set
     * using one of the methods: {@code schema}, {@code schemaLiteral},
     * {@code schemaUri}, or {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Configure the dataset's schema from a {@link File}. A schema is required,
     * and may be set using one of the methods: {@code schema},
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
     * required, and may be set using one of the methods: {@code schema},
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
     * and may be set using one of the methods: {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @param uri a URI object for the schema's location.
     * @return An instance of the builder for method chaining.
     * @throws MalformedURLException if {@code uri} is not a valid URL
     * @throws IOException
     *
     * @since 0.8.0
     */
    public Builder schemaUri(URI uri) throws IOException {
      // special support for resource URIs
      Map<String, String> match = RESOURCE_URI_PATTERN.getMatch(uri);
      if (match != null) {
        return schema(
            Resources.getResource(match.get(RESOURCE_PATH)).openStream());
      }

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

    /**
     * Configure the {@link Dataset}'s schema from a String URI. A schema is
     * required, and may be set using one of the methods: {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @param uri a String URI
     * @return An instance of the builder for method chaining.
     * @throws URISyntaxException if {@code uri} is not a valid URI
     * @throws MalformedURLException if {@code uri} is not a valid URL
     * @throws IOException
     *
     * @since 0.8.0
     */
    public Builder schemaUri(String uri) throws URISyntaxException, IOException {
      return schemaUri(new URI(uri));
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
     * required, and may be set using one of the methods: {@code schema},
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
     * required, and may be set using one of the methods: {@code schema},
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
     * data file. A schema is required, and may be set using one of the methods:
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
     * {@link InputStream}. A schema is required, and may be set using one of
     * the methods: {@code schema},  {@code schemaLiteral}, {@code schemaUri},
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
     * data file. A schema is required, and may be set using one of the methods:
     * {@code schema}, {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
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
     * not specified, {@link Formats#AVRO} will be used.
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
     * Configure the {@link Dataset}'s location (optional).
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
     * Configure the {@link Dataset}'s location (optional).
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
     * Configure the {@link Dataset}'s location (optional).
     *
     * @param uri A location String URI
     * @return An instance of the builder for method chaining.
     * @throws URISyntaxException if {@code uri} is not a valid URI
     *
     * @since 0.8.0
     */
    public Builder location(String uri) throws URISyntaxException {
      return location(new URI(uri));
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
     * Build an instance of the configured dataset descriptor. Subsequent calls
     * will produce new instances that are similarly configured.
     *
     * @since 0.9.0
     */
    public DatasetDescriptor build() {
      Preconditions.checkState(schema != null,
          "Descriptor schema is required and cannot be null");
      checkPartitionStrategy(schema, partitionStrategy);

      return new DatasetDescriptor(
          schema, schemaUrl, format, location, properties, partitionStrategy);
    }

    private static void checkPartitionStrategy(Schema schema, PartitionStrategy strategy) {
      if (strategy == null) {
        return;
      }
      Preconditions.checkState(schema.getType() == Schema.Type.RECORD,
          "Cannot partition non-records: " + schema);
      for (org.kitesdk.data.spi.FieldPartitioner fp : strategy.getFieldPartitioners()) {
        // the source name should be a field in the schema, but not necessarily
        // the record.
        Schema.Field field = schema.getField(fp.getSourceName());
        Preconditions.checkState(field != null,
            "Cannot partition on {} (missing from schema)", fp.getSourceName());
        Preconditions.checkState(
            SchemaUtil.checkType(field.schema().getType(), fp.getSourceType()),
            "Field type {} does not match partitioner {}",
            field.schema().getType(), fp);
      }
    }
  }

}
