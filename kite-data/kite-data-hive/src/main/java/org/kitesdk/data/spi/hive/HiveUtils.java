/*
 * Copyright 2013 Cloudera.
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

package org.kitesdk.data.spi.hive;

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.UnknownFormatException;
import org.kitesdk.data.impl.Accessor;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.kitesdk.data.spi.SchemaUtil;

class HiveUtils {
  static final String HDFS_SCHEME = "hdfs";

  private static final String CUSTOM_PROPERTIES_PROPERTY_NAME = "kite.custom.property.names";
  private static final String PARTITION_EXPRESSION_PROPERTY_NAME = "kite.partition.expression";
  private static final String COMPRESSION_TYPE_PROPERTY_NAME = "kite.compression.type";
  private static final String OLD_CUSTOM_PROPERTIES_PROPERTY_NAME = "cdk.custom.property.names";
  private static final String OLD_PARTITION_EXPRESSION_PROPERTY_NAME = "cdk.partition.expression";
  private static final String AVRO_SCHEMA_URL_PROPERTY_NAME = "avro.schema.url";
  private static final String AVRO_SCHEMA_LITERAL_PROPERTY_NAME = "avro.schema.literal";

  private static final Splitter NAME_SPLITTER = Splitter.on(',');
  private static final Joiner NAME_JOINER = Joiner.on(',');

  private static final Map<Format, String> FORMAT_TO_SERDE = ImmutableMap
      .<Format, String>builder()
      .put(Formats.AVRO, "org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .put(Formats.PARQUET, getHiveParquetSerde())
      .build();
  private static final Map<String, Format> SERDE_TO_FORMAT = ImmutableMap
      .<String, Format>builder()
      .put("org.apache.hadoop.hive.serde2.avro.AvroSerDe", Formats.AVRO)
      .put("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
           Formats.PARQUET)
      .put("parquet.hive.serde.ParquetHiveSerDe", Formats.PARQUET)
      .build();
  private static final Map<Format, String> FORMAT_TO_INPUT_FORMAT = ImmutableMap
      .<Format, String>builder()
      .put(Formats.AVRO,
           "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .put(Formats.PARQUET, getHiveParquetInputFormat())
      .build();
  private static final Map<Format, String> FORMAT_TO_OUTPUT_FORMAT = ImmutableMap
      .<Format, String>builder()
      .put(Formats.AVRO,
           "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .put(Formats.PARQUET, getHiveParquetOutputFormat())
      .build();

  static DatasetDescriptor descriptorForTable(Configuration conf, Table table) {
    final DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder();

    Format format;
    final String serializationLib = table.getSd().getSerdeInfo().getSerializationLib();
    if (SERDE_TO_FORMAT.containsKey(serializationLib)) {
      format = SERDE_TO_FORMAT.get(serializationLib);
      builder.format(format);
    } else {
      // TODO: should this use an "unknown" format? others fail in open()
      throw new UnknownFormatException(
          "Unknown format for serde:" + serializationLib);
    }

    final Path dataLocation = new Path(table.getSd().getLocation());
    final FileSystem fs = fsForPath(conf, dataLocation);

    builder.location(fs.makeQualified(dataLocation));

    // custom properties
    Map<String, String> properties = table.getParameters();
    String namesProperty = coalesce(
        properties.get(CUSTOM_PROPERTIES_PROPERTY_NAME),
        properties.get(OLD_CUSTOM_PROPERTIES_PROPERTY_NAME));
    if (namesProperty != null) {
      for (String property : NAME_SPLITTER.split(namesProperty)) {
        builder.property(property, properties.get(property));
      }
    }

    if (isPartitioned(table)) {
      String partitionProperty = coalesce(
          properties.get(PARTITION_EXPRESSION_PROPERTY_NAME),
          properties.get(OLD_PARTITION_EXPRESSION_PROPERTY_NAME));
      if (partitionProperty != null) {
        builder.partitionStrategy(
            Accessor.getDefault().fromExpression(partitionProperty));
      } else {
        // build a partition strategy for the table from the Hive strategy
        builder.partitionStrategy(fromPartitionColumns(getPartCols(table)));
      }
    }

    String schemaUrlString = properties.get(AVRO_SCHEMA_URL_PROPERTY_NAME);
    if (schemaUrlString != null) {
      try {
        // URI.create is safe because this library wrote the URI
        builder.schemaUri(URI.create(schemaUrlString));
      } catch (IOException e) {
        throw new DatasetIOException("Could not read schema", e);
      }
    }

    String schemaLiteral = properties.get(AVRO_SCHEMA_LITERAL_PROPERTY_NAME);
    if (schemaLiteral != null) {
      builder.schemaLiteral(schemaLiteral);
    }

    String compressionType = properties.get(COMPRESSION_TYPE_PROPERTY_NAME);
    if (compressionType != null) {
      builder.compressionType(compressionType);
    }

    try {
      return builder.build();
    } catch (IllegalStateException ex) {
      throw new DatasetException("Cannot find schema: missing metadata");
    }
  }

  private static boolean isPartitioned(Table table) {
    return (getPartCols(table).size() != 0);
  }

  private static List<FieldSchema> getPartCols(Table table) {
    List<FieldSchema> partKeys = table.getPartitionKeys();
    if (partKeys == null) {
      partKeys = new ArrayList<FieldSchema>();
      table.setPartitionKeys(partKeys);
    }
    return partKeys;
  }

  /**
   * Returns the first non-null value from the sequence or null if there is no
   * non-null value.
   */
  private static <T> T coalesce(T... values) {
    for (T value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  static Table tableForDescriptor(String namespace, String name,
                                  DatasetDescriptor descriptor,
                                  boolean external) {
    final Table table = createEmptyTable(namespace, name);

    if (external) {
      // you'd think this would do it...
      table.setTableType(TableType.EXTERNAL_TABLE.toString());
      // but it doesn't work without some additional magic:
      table.getParameters().put("EXTERNAL", "TRUE");
      table.getSd().setLocation(descriptor.getLocation().toString());
    } else {
      table.setTableType(TableType.MANAGED_TABLE.toString());
    }

    addPropertiesForDescriptor(table, descriptor);

    // translate from Format to SerDe
    final Format format = descriptor.getFormat();
    if (FORMAT_TO_SERDE.containsKey(format)) {
      table.getSd().getSerdeInfo().setSerializationLib(FORMAT_TO_SERDE.get(format));
      table.getSd().setInputFormat(FORMAT_TO_INPUT_FORMAT.get(format));
      table.getSd().setOutputFormat(FORMAT_TO_OUTPUT_FORMAT.get(format));
    } else {
      throw new UnknownFormatException(
          "No known serde for format:" + format.getName());
    }

    // copy schema info
    boolean useLiteral;
    final URL schemaURL = descriptor.getSchemaUrl();
    try {
      useLiteral = (schemaURL == null) ||
          !HDFS_SCHEME.equals(schemaURL.toURI().getScheme());
    } catch (URISyntaxException ex) {
      useLiteral = true;
    }

    if (useLiteral) {
      table.getParameters().put(
          AVRO_SCHEMA_LITERAL_PROPERTY_NAME,
          descriptor.getSchema().toString());
    } else {
      table.getParameters().put(
          AVRO_SCHEMA_URL_PROPERTY_NAME,
          schemaURL.toExternalForm());
    }

    table.getParameters().put(COMPRESSION_TYPE_PROPERTY_NAME,
        descriptor.getCompressionType().getName());

    // convert the schema to Hive columns
    table.getSd().setCols(convertSchema(descriptor.getSchema()));

    // copy partitioning info
    if (descriptor.isPartitioned()) {
      PartitionStrategy ps = descriptor.getPartitionStrategy();
      table.getParameters().put(PARTITION_EXPRESSION_PROPERTY_NAME,
          Accessor.getDefault().toExpression(ps));
      table.setPartitionKeys(partitionColumns(ps, descriptor.getSchema()));
    }

    return table;
  }

  static Table createEmptyTable(String namespace, String name) {
    Table table = new Table();
    table.setDbName(namespace);
    table.setTableName(name);
    table.setPartitionKeys(new ArrayList<FieldSchema>());
    table.setParameters(new HashMap<String, String>());

    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo());
    sd.setNumBuckets(-1);
    sd.setBucketCols(new ArrayList<String>());
    sd.setCols(new ArrayList<FieldSchema>());
    sd.setParameters(new HashMap<String, String>());
    sd.setSortCols(new ArrayList<Order>());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    SkewedInfo skewInfo = new SkewedInfo();
    skewInfo.setSkewedColNames(new ArrayList<String>());
    skewInfo.setSkewedColValues(new ArrayList<List<String>>());
    skewInfo.setSkewedColValueLocationMaps(new HashMap<List<String>, String>());
    sd.setSkewedInfo(skewInfo);
    table.setSd(sd);

    return table;
  }

  public static void updateTableSchema(Table table, DatasetDescriptor descriptor) {
    if (table.getParameters().get(AVRO_SCHEMA_LITERAL_PROPERTY_NAME) != null) {
      table.getParameters().put(
          AVRO_SCHEMA_LITERAL_PROPERTY_NAME,
          descriptor.getSchema().toString());
    } else if (table.getParameters().get(AVRO_SCHEMA_URL_PROPERTY_NAME) != null) {
      if (descriptor.getSchemaUrl() == null) {
        throw new DatasetException("Cannot update " + AVRO_SCHEMA_URL_PROPERTY_NAME +
            " since descriptor schema URL is not set.");
      }
      table.getParameters().put(
          AVRO_SCHEMA_URL_PROPERTY_NAME,
          descriptor.getSchemaUrl().toExternalForm());
    } else {
      throw new DatasetException("Cannot update Avro schema since neither " +
          AVRO_SCHEMA_LITERAL_PROPERTY_NAME + " nor " +
          AVRO_SCHEMA_URL_PROPERTY_NAME + " is set.");
    }
    // keep the custom properties up-to-date
    addPropertiesForDescriptor(table, descriptor);
  }

  static FileSystem fsForPath(Configuration conf, Path path) {
    try {
      return path.getFileSystem(conf);
    } catch (IOException ex) {
      throw new DatasetIOException("Cannot access FileSystem for uri:" + path, ex);
    }
  }

  private static void addPropertiesForDescriptor(Table table,
                                                 DatasetDescriptor descriptor) {
    // copy custom properties to the table
    if (!descriptor.listProperties().isEmpty()) {
      for (String property : descriptor.listProperties()) {
        // no need to check the reserved list, those are not set on descriptors
        table.getParameters().put(property, descriptor.getProperty(property));
      }
      // set which properties are custom and should be set on descriptors
      table.getParameters().put(CUSTOM_PROPERTIES_PROPERTY_NAME,
          NAME_JOINER.join(descriptor.listProperties()));
    }
  }

  /**
   * Returns the correct dataset path for the given name and root directory.
   *
   * @param root A Path
   * @param namespace A String namespace, or logical group
   * @param name A String dataset name
   * @return the correct dataset Path
   */
  static Path pathForDataset(Path root, String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    // Why replace '.' here? Is this a namespacing hack?
    return new Path(root, new Path(namespace, name.replace('.', Path.SEPARATOR_CHAR)));
  }

  static List<FieldSchema> partitionColumns(PartitionStrategy strategy, Schema schema) {
    List<FieldSchema> columns = Lists.newArrayList();
    for (FieldPartitioner<?, ?> fp : strategy.getFieldPartitioners()) {
      columns.add(new FieldSchema(fp.getName(),
          getHiveType(SchemaUtil.getPartitionType(fp, schema)),
          "Partition column derived from '" + fp.getSourceName() + "' column, " +
              "generated by Kite."));
    }
    return columns;
  }

  static PartitionStrategy fromPartitionColumns(List<FieldSchema> fields) {
    throw new UnsupportedOperationException(
        "Cannot build partition strategy from Hive table");
    // this will be possible with provided field partitioners
  }

  private static String getHiveType(Class<?> type) {
    String typeName = PrimitiveObjectInspectorUtils.getTypeNameFromPrimitiveJava(type);
    if (typeName == null) {
      throw new DatasetException("Unsupported FieldPartitioner type: " + type);
    }
    return typeName;
  }

  static List<FieldSchema> convertSchema(Schema avroSchema) {
    List<FieldSchema> columns = Lists.newArrayList();
    if (Schema.Type.RECORD.equals(avroSchema.getType())) {
      for (Field field : avroSchema.getFields()) {
        columns.add(new FieldSchema(
            field.name(), convert(field.schema()).getTypeName(), field.doc()));
      }
    } else {
      columns.add(new FieldSchema(
          "column", convert(avroSchema).getTypeName(), avroSchema.getDoc()));
    }
    return columns;
  }

  public static TypeInfo convert(Schema schema) {
    return SchemaUtil.visit(schema, new Converter());
  }

  static class Converter extends SchemaUtil.SchemaVisitor<TypeInfo> {
    private static DynMethods.StaticMethod hiveTypeForName =
        new DynMethods.Builder("getPrimitiveTypeInfo")
            .impl(TypeInfoFactory.class, String.class)
            .buildStatic();

    static final ImmutableMap<Schema.Type, TypeInfo> TYPE_TO_TYPEINFO =
        ImmutableMap.<Schema.Type, TypeInfo>builder()
            .put(Schema.Type.BOOLEAN, hiveTypeForName.<TypeInfo>invoke("boolean"))
            .put(Schema.Type.INT, hiveTypeForName.<TypeInfo>invoke("int"))
            .put(Schema.Type.LONG, hiveTypeForName.<TypeInfo>invoke("bigint"))
            .put(Schema.Type.FLOAT, hiveTypeForName.<TypeInfo>invoke("float"))
            .put(Schema.Type.DOUBLE, hiveTypeForName.<TypeInfo>invoke("double"))
            .put(Schema.Type.STRING, hiveTypeForName.<TypeInfo>invoke("string"))
            .put(Schema.Type.ENUM, hiveTypeForName.<TypeInfo>invoke("string"))
            .put(Schema.Type.BYTES, hiveTypeForName.<TypeInfo>invoke("binary"))
            .put(Schema.Type.FIXED, hiveTypeForName.<TypeInfo>invoke("binary"))
            .build();

    public TypeInfo record(Schema record, List<String> names, List<TypeInfo> types) {
      return TypeInfoFactory.getStructTypeInfo(names, types);
    }

    @Override
    public TypeInfo union(Schema union, List<TypeInfo> options) {
      boolean nullable = false;
      List<TypeInfo> nonNullTypes = Lists.newArrayList();
      for (TypeInfo type : options) {
        if (type != null) {
          nonNullTypes.add(type);
        } else {
          nullable = true;
        }
      }

      // handle a single field in the union
      if (nonNullTypes.size() == 1) {
        return nonNullTypes.get(0);
      }

      // TODO: where does nullability get passed?

      return TypeInfoFactory.getUnionTypeInfo(nonNullTypes);
    }

    @Override
    public TypeInfo array(Schema array, TypeInfo element) {
      return TypeInfoFactory.getListTypeInfo(element);
    }

    @Override
    public TypeInfo map(Schema map, TypeInfo value) {
      return TypeInfoFactory.getMapTypeInfo(
          TYPE_TO_TYPEINFO.get(Schema.Type.STRING), value);
    }

    @Override
    public TypeInfo primitive(Schema primitive) {
      return TYPE_TO_TYPEINFO.get(primitive.getType());
    }
  }

  private static String getHiveParquetInputFormat() {
    String newClass = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    String oldClass = "parquet.hive.DeprecatedParquetInputFormat";

    try {
      Class.forName(newClass);
      return newClass;
    } catch (ClassNotFoundException ex) {
      return oldClass;
    }
  }

  private static String getHiveParquetOutputFormat() {
    String newClass = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    String oldClass = "parquet.hive.DeprecatedParquetOutputFormat";

    try {
      Class.forName(newClass);
      return newClass;
    } catch (ClassNotFoundException ex) {
      return oldClass;
    }
  }

  private static String getHiveParquetSerde() {
    String newClass = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    String oldClass = "parquet.hive.serde.ParquetHiveSerDe";

    try {
      Class.forName(newClass);
      return newClass;
    } catch (ClassNotFoundException ex) {
      return oldClass;
    }
  }
}
