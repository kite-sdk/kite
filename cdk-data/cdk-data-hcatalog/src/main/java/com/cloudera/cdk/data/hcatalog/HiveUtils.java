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

package com.cloudera.cdk.data.hcatalog;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.Format;
import com.cloudera.cdk.data.Formats;
import com.cloudera.cdk.data.MetadataProviderException;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.UnknownFormatException;
import com.cloudera.cdk.data.impl.Accessor;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

class HiveUtils {
  static final String DEFAULT_DB = "default";
  static final String HDFS_SCHEME = "hdfs";

  private static final String CUSTOM_PROPERTIES_PROPERTY_NAME = "cdk.custom.property.names";
  private static final String PARTITION_EXPRESSION_PROPERTY_NAME = "cdk.partition.expression";
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
    //FORMAT_TO_SERDE.put(Formats.PARQUET, "parquet.hive.serde.ParquetHiveSerDe");

    FORMAT_TO_INPUT_FORMAT.put(Formats.AVRO, "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
    //FORMAT_TO_INPUT_FORMAT.put(Formats.PARQUET, "parquet.hadoop.ParquetInputFormat");

    FORMAT_TO_OUTPUT_FORMAT.put(Formats.AVRO, "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
    //FORMAT_TO_INPUT_FORMAT.put(Formats.PARQUET, "parquet.hadoop.ParquetOutputFormat");
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
    Properties props = table.getMetadata();
    String namesProperty = props.getProperty(CUSTOM_PROPERTIES_PROPERTY_NAME);
    if (namesProperty != null) {
      for (String property : NAME_SPLITTER.split(namesProperty)) {
        builder.property(property, props.getProperty(property));
      }
    }

    if (table.getProperty(PARTITION_EXPRESSION_PROPERTY_NAME) != null) {
      builder.partitionStrategy(
          Accessor.getDefault().fromExpression(
              table.getProperty(PARTITION_EXPRESSION_PROPERTY_NAME)));
    }

    String schemaUrlString = table.getProperty(AVRO_SCHEMA_URL_PROPERTY_NAME);
    if (schemaUrlString != null) {
      try {
        builder.schema(new URI(schemaUrlString));
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
      return builder.get();
    }  catch (IllegalStateException ex) {
      throw new MetadataProviderException("Cannot find schema: missing metadata");
    }
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

    // copy partitioning info
    if (descriptor.isPartitioned()) {
      PartitionStrategy ps = descriptor.getPartitionStrategy();
      table.setProperty(PARTITION_EXPRESSION_PROPERTY_NAME,
          Accessor.getDefault().toExpression(ps));
      List<FieldSchema> partCols = Lists.newArrayList();
      for (FieldPartitioner fp : ps.getFieldPartitioners()) {
        partCols.add(new FieldSchema(fp.getName(), getHiveType(fp.getType()),
            "Partition column derived from '" + fp.getSourceName() + "' column, " +
                "generated by CDK."));
      }
      table.setPartCols(partCols);
    }

    return table;
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

}
