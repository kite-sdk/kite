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
package org.kitesdk.data.hbase;

import com.google.common.base.Preconditions;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.MetadataProviderException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.hbase.avro.AvroEntitySchema;
import org.kitesdk.data.hbase.avro.AvroKeyEntitySchemaParser;
import org.kitesdk.data.hbase.impl.Constants;
import org.kitesdk.data.hbase.impl.EntitySchema;
import org.kitesdk.data.hbase.impl.SchemaManager;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.AbstractMetadataProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.kitesdk.data.spi.Compatibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HBaseMetadataProvider extends AbstractMetadataProvider {

  private static final Logger logger = LoggerFactory
      .getLogger(HBaseMetadataProvider.class);

  private HBaseAdmin hbaseAdmin;
  private SchemaManager schemaManager;

  public HBaseMetadataProvider(HBaseAdmin hbaseAdmin, SchemaManager schemaManager) {
    this.hbaseAdmin = hbaseAdmin;
    this.schemaManager = schemaManager;
  }

  @Override
  public DatasetDescriptor create(String name, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");
    Compatibility.checkAndWarn(
        HBaseMetadataProvider.getEntityName(name),
        descriptor.getSchema());

    try {
      String managedSchemaName = "managed_schemas"; // TODO: allow table to be specified
      if (!hbaseAdmin.tableExists(managedSchemaName)) {
        HTableDescriptor table = new HTableDescriptor(managedSchemaName);
        table.addFamily(new HColumnDescriptor("meta"));
        table.addFamily(new HColumnDescriptor("schema"));
        table.addFamily(new HColumnDescriptor("_s"));
        hbaseAdmin.createTable(table);
      }
    } catch (IOException e) {
      throw new DatasetException(e);
    }

    String entitySchemaString = descriptor.getSchema().toString(true);

    AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
    AvroEntitySchema entitySchema = parser.parseEntitySchema(entitySchemaString);

    String tableName = getTableName(name);
    String entityName = getEntityName(name);

    schemaManager.refreshManagedSchemaCache(tableName, entityName);
    schemaManager.createSchema(tableName, entityName,
        entitySchemaString,
        "org.kitesdk.data.hbase.avro.AvroKeyEntitySchemaParser",
        "org.kitesdk.data.hbase.avro.AvroKeySerDe",
        "org.kitesdk.data.hbase.avro.AvroEntitySerDe");

    try {
      if (!hbaseAdmin.tableExists(tableName)) {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor(Constants.SYS_COL_FAMILY));
        desc.addFamily(new HColumnDescriptor(Constants.OBSERVABLE_COL_FAMILY));
        for (String columnFamily : entitySchema.getRequiredColumnFamilies()) {
          desc.addFamily(new HColumnDescriptor(columnFamily));
        }
        hbaseAdmin.createTable(desc);
      } else {
        Set<String> familiesToAdd = entitySchema.getRequiredColumnFamilies();
        familiesToAdd.add(new String(Constants.SYS_COL_FAMILY));
        familiesToAdd.add(new String(Constants.OBSERVABLE_COL_FAMILY));
        HTableDescriptor desc = hbaseAdmin.getTableDescriptor(tableName
            .getBytes());
        for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
          String familyName = columnDesc.getNameAsString();
          if (familiesToAdd.contains(familyName)) {
            familiesToAdd.remove(familyName);
          }
        }
        if (familiesToAdd.size() > 0) {
          hbaseAdmin.disableTable(tableName);
          try {
            for (String family : familiesToAdd) {
              hbaseAdmin.addColumn(tableName, new HColumnDescriptor(family));
            }
          } finally {
            hbaseAdmin.enableTable(tableName);
          }
        }
      }
    } catch (IOException e) {
      throw new DatasetException(e);
    }
    return withPartitionStrategy(descriptor);
  }

  @Override
  public DatasetDescriptor update(String name, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");
    Compatibility.checkAndWarn(
        HBaseMetadataProvider.getEntityName(name),
        descriptor.getSchema());

    String tableName = getTableName(name);
    String entityName = getEntityName(name);
    schemaManager.refreshManagedSchemaCache(tableName, entityName);
    String schemaString = descriptor.getSchema().toString();
    AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
    EntitySchema entitySchema = parser.parseEntitySchema(schemaString);
    if (!schemaManager.hasSchemaVersion(tableName, entityName, entitySchema)) {
      schemaManager.migrateSchema(tableName, entityName, schemaString);
    } else {
      logger.info("Schema hasn't changed, not migrating: (" + name + ")");
    }
    return withPartitionStrategy(descriptor);
  }

  @Override
  public DatasetDescriptor load(String name) {
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    if (!exists(name)) {
      throw new DatasetNotFoundException("No such dataset: " + name);
    }
    String tableName = getTableName(name);
    String entityName = getEntityName(name);
    return getDatasetDescriptor(schemaManager.getEntitySchema(tableName, entityName).getRawSchema());
  }

  @Override
  public boolean delete(String name) {
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    DatasetDescriptor descriptor;
    try {
      descriptor = load(name);
    } catch (DatasetNotFoundException e) {
      return false;
    }

    String tableName = getTableName(name);
    String entityName = getEntityName(name);

    schemaManager.deleteSchema(tableName, entityName);

    String entitySchemaString = descriptor.getSchema().toString(true);

    AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
    AvroEntitySchema entitySchema = parser.parseEntitySchema(entitySchemaString);

    // TODO: this may delete columns for other entities if they share column families
    // TODO: https://issues.cloudera.org/browse/CDK-145, https://issues.cloudera.org/browse/CDK-146
    for (String columnFamily : entitySchema.getRequiredColumnFamilies()) {
      try {
        hbaseAdmin.disableTable(tableName);
        try {
          hbaseAdmin.deleteColumn(tableName, columnFamily);
        } finally {
          hbaseAdmin.enableTable(tableName);
        }
      } catch (IOException e) {
        throw Accessor.getDefault().providerExceptionFor(
            new DatasetIOException("Cannot delete " + name, e));
      }
    }
    return true;
  }

  @Override
  public boolean exists(String name) {
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    String tableName = getTableName(name);
    String entityName = getEntityName(name);
    schemaManager.refreshManagedSchemaCache(tableName, entityName);
    return schemaManager.hasManagedSchema(tableName, entityName);
  }

  public Collection<String> list() {
    throw new UnsupportedOperationException();
  }

  static String getTableName(String name) {
    // TODO: change to use namespace (CDK-140)
    if (name.contains(".")) {
      return name.substring(0, name.indexOf('.'));
    }
    return name;
  }

  static String getEntityName(String name) {
    return name.substring(name.indexOf('.') + 1);
  }

  private static DatasetDescriptor getDatasetDescriptor(String schemaString) {
    AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
    PartitionStrategy partitionStrategy = parser.parseKeySchema(schemaString)
        .getPartitionStrategy();
    return new DatasetDescriptor.Builder()
        .schemaLiteral(schemaString)
        .partitionStrategy(partitionStrategy)
        .build();
  }

  // TODO: move the logic of parsing keys to DatasetDescriptor itself
  private static DatasetDescriptor withPartitionStrategy(DatasetDescriptor descriptor) {
    AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
    PartitionStrategy partitionStrategy = parser.parseKeySchema(descriptor.getSchema().toString())
        .getPartitionStrategy();
    return new DatasetDescriptor.Builder()
        .schema(descriptor.getSchema())
        .partitionStrategy(partitionStrategy)
        .location(descriptor.getLocation())
        .build();
  }

}
