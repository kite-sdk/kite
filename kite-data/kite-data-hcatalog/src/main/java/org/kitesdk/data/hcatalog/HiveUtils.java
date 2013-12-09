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

package org.kitesdk.data.hcatalog;

import com.google.common.base.Objects;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.MetadataProviderException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.UnknownFormatException;
import org.kitesdk.data.impl.Accessor;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

class HiveUtils {
  static final String DEFAULT_DB = "default";
  static final String HDFS_SCHEME = "hdfs";

  private static final String CUSTOM_PROPERTIES_PROPERTY_NAME = "kite.custom.property.names";
  private static final String PARTITION_EXPRESSION_PROPERTY_NAME = "kite.partition.expression";
  private static final String OLD_CUSTOM_PROPERTIES_PROPERTY_NAME = "cdk.custom.property.names";
  private static final String OLD_PARTITION_EXPRESSION_PROPERTY_NAME = "cdk.partition.expression";
  private static final String AVRO_SCHEMA_URL_PROPERTY_NAME = "avro.schema.url";
  private static final String AVRO_SCHEMA_LITERAL_PROPERTY_NAME = "avro.schema.literal";

  private static final Splitter NAME_SPLITTER = Splitter.on(',');
  private static final Joiner NAME_JOINER = Joiner.on(',');

  private static final BiMap<Format, String> FORMAT_TO_SERDE = HashBiMap.create(2);
  private static final BiMap<String, Format> SERDE_TO_FORMAT = FORMAT_TO_SERDE.inverse();
  private static final Map<Format, String> FORMAT_TO_INPUT_FORMAT = Maps.newHashMap();
  private static final Map<Format, String> FORMAT_TO_OUTPUT_FORMAT = Maps.newHashMap();
  static {
    FORMAT_TO_SERDE.put(Formats.AVRO, "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
    FORMAT_TO_SERDE.put(Formats.PARQUET, "parquet.hive.serde.ParquetHiveSerDe");

    FORMAT_TO_INPUT_FORMAT.put(Formats.AVRO, "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
    FORMAT_TO_INPUT_FORMAT.put(Formats.PARQUET, "parquet.hive.DeprecatedParquetInputFormat");

    FORMAT_TO_OUTPUT_FORMAT.put(Formats.AVRO, "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
    FORMAT_TO_OUTPUT_FORMAT.put(Formats.PARQUET, "parquet.hive.DeprecatedParquetOutputFormat");
  }

  static DatasetDescriptor descriptorForTable(Configuration conf, Table table) {
    final DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder();

    final String serializationLib = table.getSerializationLib();
    if (SERDE_TO_FORMAT.containsKey(serializationLib)) {
      builder.format(SERDE_TO_FORMAT.get(serializationLib));
    } else {
      // TODO: should this use an "unknown" format? others fail in open()
      throw new UnknownFormatException(
          "Unknown format for serde:" + serializationLib);
    }

    final Path dataLocation = new Path(table.getDataLocation());
    final FileSystem fs = fsForPath(conf, dataLocation);

    builder.location(fs.makeQualified(dataLocation));

    // custom properties
    String namesProperty = coalesce(
        table.getProperty(CUSTOM_PROPERTIES_PROPERTY_NAME),
        table.getProperty(OLD_CUSTOM_PROPERTIES_PROPERTY_NAME));
    if (namesProperty != null) {
      for (String property : NAME_SPLITTER.split(namesProperty)) {
        builder.property(property, table.getProperty(property));
      }
    }

    String partitionProperty = coalesce(
        table.getProperty(PARTITION_EXPRESSION_PROPERTY_NAME),
        table.getProperty(OLD_PARTITION_EXPRESSION_PROPERTY_NAME));
    if (partitionProperty != null) {
      builder.partitionStrategy(
          Accessor.getDefault().fromExpression(partitionProperty));
    }

    String schemaUrlString = table.getProperty(AVRO_SCHEMA_URL_PROPERTY_NAME);
    if (schemaUrlString != null) {
      try {
        builder.schemaUri(new URI(schemaUrlString));
      } catch (IOException e) {
        throw new MetadataProviderException("Could not read schema", e);
      } catch (URISyntaxException e) {
        // this library sets the URI, so it should always be valid
        throw new MetadataProviderException("[BUG] Invalid schema URI", e);
      }
    }

    String schemaLiteral = table.getProperty(AVRO_SCHEMA_LITERAL_PROPERTY_NAME);
    if (schemaLiteral != null) {
      builder.schemaLiteral(schemaLiteral);
    }

    try {
      return builder.build();
    }  catch (IllegalStateException ex) {
      throw new MetadataProviderException("Cannot find schema: missing metadata");
    }
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

  static Table tableForDescriptor(
      String name, DatasetDescriptor descriptor, boolean external) {
    final Table table = new Table(DEFAULT_DB, name);

    if (external) {
      // you'd think this would do it...
      table.setTableType(TableType.EXTERNAL_TABLE);
      // but it doesn't work without some additional magic:
      table.getParameters().put("EXTERNAL", "TRUE");
      table.setDataLocation(descriptor.getLocation());
    } else {
      table.setTableType(TableType.MANAGED_TABLE);
    }

    // copy custom properties to the table
    if (!descriptor.listProperties().isEmpty()) {
      for (String property : descriptor.listProperties()) {
        // no need to check the reserved list, those are not set on descriptors
        table.setProperty(property, descriptor.getProperty(property));
      }
      // set which properties are custom and should be set on descriptors
      table.setProperty(CUSTOM_PROPERTIES_PROPERTY_NAME,
          NAME_JOINER.join(descriptor.listProperties()));
    }

    // translate from Format to SerDe
    final Format format = descriptor.getFormat();
    if (FORMAT_TO_SERDE.containsKey(format)) {
      table.setSerializationLib(FORMAT_TO_SERDE.get(format));
      try {
        table.setInputFormatClass(FORMAT_TO_INPUT_FORMAT.get(format));
        table.setOutputFormatClass(FORMAT_TO_OUTPUT_FORMAT.get(format));
      } catch (HiveException ex) {
        throw new MetadataProviderException(
            "Failed to set input/output formats for format:" +
            format.getName(), ex);
      }
    } else {
      throw new UnknownFormatException(
          "No known serde for format:" + format.getName());
    }
    try {
    } catch (Exception e) {
      throw new MetadataProviderException("Error configuring Hive Avro table, " +
          "table creation failed", e);
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
      table.setProperty(
          AVRO_SCHEMA_LITERAL_PROPERTY_NAME,
          descriptor.getSchema().toString());
    } else {
      table.setProperty(
          AVRO_SCHEMA_URL_PROPERTY_NAME,
          schemaURL.toExternalForm());
    }

    // convert the schema to Hive columns
    table.setFields(convertSchema(descriptor.getSchema()));

    // copy partitioning info
    if (descriptor.isPartitioned()) {
      PartitionStrategy ps = descriptor.getPartitionStrategy();
      table.setProperty(PARTITION_EXPRESSION_PROPERTY_NAME,
          Accessor.getDefault().toExpression(ps));
      List<FieldSchema> partCols = Lists.newArrayList();
      for (FieldPartitioner fp : ps.getFieldPartitioners()) {
        partCols.add(new FieldSchema(fp.getName(), getHiveType(fp.getType()),
            "Partition column derived from '" + fp.getSourceName() + "' column, " +
                "generated by Kite."));
      }
      table.setPartCols(partCols);
    }

    return table;
  }


  public static void updateTableSchema(Table table, DatasetDescriptor descriptor) {
    if (table.getProperty(AVRO_SCHEMA_LITERAL_PROPERTY_NAME) != null) {
      table.setProperty(
          AVRO_SCHEMA_LITERAL_PROPERTY_NAME,
          descriptor.getSchema().toString());
    } else if (table.getProperty(AVRO_SCHEMA_URL_PROPERTY_NAME) != null) {
      if (descriptor.getSchemaUrl() == null) {
        throw new MetadataProviderException("Cannot update " +
            AVRO_SCHEMA_URL_PROPERTY_NAME + " since descriptor schema URL is not set.");
      }
      table.setProperty(
          AVRO_SCHEMA_URL_PROPERTY_NAME,
          descriptor.getSchemaUrl().toExternalForm());
    } else {
      throw new MetadataProviderException("Cannot update Avro schema since neither " +
          AVRO_SCHEMA_LITERAL_PROPERTY_NAME + " nor " + AVRO_SCHEMA_URL_PROPERTY_NAME +
          " is set.");
    }
  }

  static FileSystem fsForPath(Configuration conf, Path path) {
    try {
      return path.getFileSystem(conf);
    } catch (IOException ex) {
      throw new MetadataProviderException(
          "Cannot access FileSystem for uri:" + path, ex);
    }
  }

  static FileSystem fsForPath(Configuration conf, String fsUri, Path path) {
    try {
      if (fsUri == null) {
        return path.getFileSystem(conf);
      } else {
        return FileSystem.get(new URI(fsUri), conf);
      }
    } catch (IOException ex) {
      throw new MetadataProviderException(
          "Cannot access FileSystem for path:" + path, ex);
    } catch (URISyntaxException ex) {
      throw new MetadataProviderException(
          "Cannot access FileSystem for uri:" + fsUri, ex);
    }
  }

  /**
   * Returns the correct dataset path for the given name and root directory.
   *
   * @param root A Path
   * @param name A String dataset name
   * @return the correct dataset Path
   */
  static Path pathForDataset(Path root, String name) {
    Preconditions.checkArgument(name != null, "Dataset name cannot be null");

    // Why replace '.' here? Is this a namespacing hack?
    return new Path(root, name.replace('.', Path.SEPARATOR_CHAR));
  }

  static String getHiveType(Class<?> type) {
    String typeName = PrimitiveObjectInspectorUtils.getTypeNameFromPrimitiveJava(type);
    if (typeName == null) {
      throw new MetadataProviderException("Unsupported FieldPartitioner type: " + type);
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

  static TypeInfo convert(Schema avroSchema) {
    // TODO: throw an error on recursive types
    switch (avroSchema.getType()) {
      case RECORD:
        return convertRecord(avroSchema);
      case UNION:
        return convertUnion(avroSchema);
      case ARRAY:
        return convertArray(avroSchema);
      case MAP:
        return convertMap(avroSchema);
      default:
        return convertPrimitive(avroSchema);
    }
  }

  static TypeInfo convertRecord(Schema avroRecord) {
    Preconditions.checkArgument(
        Schema.Type.RECORD.equals(avroRecord.getType()),
        "Avro Schema must be a Record");

    final List<Field> fields = avroRecord.getFields();
    final List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
    final List<TypeInfo> types = Lists.newArrayListWithExpectedSize(fields.size());
    for (Field field : fields) {
      names.add(field.name());
      types.add(convert(field.schema()));
    }

    return TypeInfoFactory.getStructTypeInfo(names, types);
  }

  static TypeInfo convertUnion(Schema avroUnion) {
    Preconditions.checkArgument(
        Schema.Type.UNION.equals(avroUnion.getType()),
        "Avro Schema must be a Union");

    final List<TypeInfo> nonNullTypes = Lists.newArrayList();
    for (Schema schema : avroUnion.getTypes()) {
      if (!Schema.Type.NULL.equals(schema.getType())) {
        nonNullTypes.add(convert(schema));
      }
    }

    // check for a single, possible nullable field
    if (nonNullTypes.size() == 1) {
      // TODO: where does nullability get passed?
      // nullable if avroUnion.getTypes().size() == 2
      return nonNullTypes.get(0);
    }

    return TypeInfoFactory.getUnionTypeInfo(nonNullTypes);
  }

  static TypeInfo convertArray(Schema avroArray) {
    Preconditions.checkArgument(
        Schema.Type.ARRAY.equals(avroArray.getType()),
        "Avro Schema must be an Array");

    return TypeInfoFactory.getListTypeInfo(convert(avroArray.getElementType()));
  }

  static TypeInfo convertMap(Schema avroMap) {
    Preconditions.checkArgument(
        Schema.Type.MAP.equals(avroMap.getType()),
        "Avro Schema must be a Map");

    return TypeInfoFactory.getMapTypeInfo(
        TypeInfoFactory.stringTypeInfo, convert(avroMap.getValueType()));
  }

  static final ImmutableMap<Schema.Type, TypeInfo> TYPE_TO_TYPEINFO =
      ImmutableMap.<Schema.Type, TypeInfo>builder()
          .put(Schema.Type.BOOLEAN, TypeInfoFactory.booleanTypeInfo)
          .put(Schema.Type.INT, TypeInfoFactory.intTypeInfo)
          .put(Schema.Type.LONG, TypeInfoFactory.longTypeInfo)
          .put(Schema.Type.FLOAT, TypeInfoFactory.floatTypeInfo)
          .put(Schema.Type.DOUBLE, TypeInfoFactory.doubleTypeInfo)
          .put(Schema.Type.STRING, TypeInfoFactory.stringTypeInfo)
          .put(Schema.Type.ENUM, TypeInfoFactory.stringTypeInfo)
          .put(Schema.Type.BYTES, TypeInfoFactory.binaryTypeInfo)
          .put(Schema.Type.FIXED, TypeInfoFactory.binaryTypeInfo)
          .build();

  static TypeInfo convertPrimitive(Schema avroPrimitive) {
    TypeInfo type = TYPE_TO_TYPEINFO.get(avroPrimitive.getType());

    Preconditions.checkArgument(type != null,
        "Avro Schema must be a primitive type, not " + avroPrimitive.getType());

    return type;
  }

}
