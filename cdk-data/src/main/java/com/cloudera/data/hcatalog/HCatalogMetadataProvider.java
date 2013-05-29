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
package com.cloudera.data.hcatalog;

import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.MetadataProvider;
import com.cloudera.data.MetadataProviderException;
import java.io.IOException;
import java.net.URL;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HCatalogMetadataProvider implements MetadataProvider {

  private static final Logger logger = LoggerFactory
      .getLogger(HCatalogMetadataProvider.class);

  private static final String AVRO_SERDE = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";

  private final boolean managed;
  private final String dbName = "default";
  private FileSystem fileSystem;
  private Path dataDirectory;

  public HCatalogMetadataProvider(boolean managed) {
    this.managed = managed;
  }

  @Override
  public DatasetDescriptor load(String name) {
    Table table = HCatalog.getTable(dbName, name);
    String serializationLib = table.getSerializationLib();
    if (!AVRO_SERDE.equals(serializationLib)) {
      throw new MetadataProviderException("Only tables using AvroSerDe are supported.");
    }
    try {
      fileSystem = FileSystem.get(new Configuration());
      dataDirectory = fileSystem.makeQualified(new Path(table.getDataLocation()));
    } catch (IOException e) {
      throw new MetadataProviderException(e);
    }
    String schemaUrlString = table.getProperty("avro.schema.url");
    if (schemaUrlString != null) {
      try {
        URL schemaUrl = new URL(schemaUrlString);
        return new DatasetDescriptor.Builder().schema(schemaUrl).get();
      } catch (IOException e) {
        throw new MetadataProviderException(e);
      }
    }
    String schemaLiteral = table.getProperty("avro.schema.literal");
    if (schemaLiteral != null) {
      return new DatasetDescriptor.Builder().schema(schemaLiteral).get();
    }
    throw new MetadataProviderException("Can't find schema.");
  }

  FileSystem getFileSystem() {
    return fileSystem;
  }

  Path getDataDirectory() {
    return dataDirectory;
  }

  void setFileSystem(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  void setDataDirectory(Path dataDirectory) {
    this.dataDirectory = dataDirectory;
  }

  @Override
  public void save(String name, DatasetDescriptor descriptor) {
    if (HCatalog.tableExists(dbName, name)) {
      logger.warn("Hive table named " + name + " already exists");
      return;
    }
    logger.info("Creating a Hive table named: " + name);
    Table tbl = new Table(dbName, name);
    tbl.setTableType(managed ? TableType.MANAGED_TABLE : TableType.EXTERNAL_TABLE);
    try {
      if (dataDirectory != null) {
        tbl.setDataLocation(fileSystem.makeQualified(dataDirectory).toUri());
      }
      tbl.setSerializationLib(AVRO_SERDE);
      tbl.setInputFormatClass("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
      tbl.setOutputFormatClass("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
      if (descriptor.getSchemaUrl() != null) {
        tbl.setProperty("avro.schema.url", descriptor.getSchemaUrl().toExternalForm());
      } else {
        tbl.setProperty("avro.schema.literal", descriptor.getSchema().toString());
      }
    } catch (Exception e) {
      throw new MetadataProviderException("Error configuring Hive Avro table, " +
          "table creation failed", e);
    }
    HCatalog.createTable(tbl);

    if (dataDirectory == null) { // re-read to find the data directory
      Table table = HCatalog.getTable(dbName, name);
      try {
        fileSystem = FileSystem.get(new Configuration());
        dataDirectory = fileSystem.makeQualified(new Path(table.getDataLocation()));
      } catch (IOException e) {
        throw new MetadataProviderException(e);
      }
    }
  }

  @Override
  public boolean delete(String name) {
    if (!HCatalog.tableExists(dbName, name)) {
      return false;
    }
    HCatalog.dropTable(dbName, name);
    return true;
  }

}
