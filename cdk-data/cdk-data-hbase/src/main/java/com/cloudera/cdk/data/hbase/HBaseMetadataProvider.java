package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetRepositoryException;
import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.NoSuchDatasetException;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.dao.Constants;
import com.cloudera.cdk.data.dao.HBaseCommonException;
import com.cloudera.cdk.data.dao.SchemaManager;
import com.cloudera.cdk.data.hbase.avro.impl.AvroEntitySchema;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeyEntitySchemaParser;
import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
import com.cloudera.cdk.data.hbase.manager.DefaultSchemaManager;
import com.cloudera.cdk.data.spi.AbstractMetadataProvider;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.codehaus.jackson.JsonNode;

public class HBaseMetadataProvider extends AbstractMetadataProvider {

  private HBaseAdmin hbaseAdmin;
  private SchemaManager schemaManager;

  public HBaseMetadataProvider(HBaseAdmin hbaseAdmin, HTablePool tablePool) {
    this.hbaseAdmin = hbaseAdmin;
    this.schemaManager = new DefaultSchemaManager(tablePool);
  }

  @Override
  public DatasetDescriptor create(String name, DatasetDescriptor descriptor) {

    String entitySchemaString = descriptor.getSchema().toString(true);

    AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
    AvroEntitySchema entitySchema = parser.parseEntitySchema(entitySchemaString);

    String tableName = getTableName(name);
    String entityName = getEntityName(name);

    schemaManager.refreshManagedSchemaCache(tableName, entityName);
    schemaManager.createSchema(tableName, entityName,
        entitySchemaString,
        "com.cloudera.cdk.data.hbase.avro.impl.AvroKeyEntitySchemaParser",
        "com.cloudera.cdk.data.hbase.avro.impl.AvroKeySerDe",
        "com.cloudera.cdk.data.hbase.avro.impl.AvroEntitySerDe");

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
      throw new HBaseCommonException(e);
    }
    return descriptor;
  }

  @Override
  public DatasetDescriptor update(String name, DatasetDescriptor descriptor) {
    String tableName = getTableName(name);
    String entityName = getEntityName(name);
    schemaManager.refreshManagedSchemaCache(tableName, entityName);
    schemaManager.migrateSchema(tableName, entityName,
        descriptor.getSchema().toString());
    return descriptor;
  }

  @Override
  public DatasetDescriptor load(String name) {
    if (!exists(name)) {
      throw new NoSuchDatasetException("No such dataset: " + name);
    }
    String tableName = getTableName(name);
    String entityName = getEntityName(name);
    return getDatasetDescriptor(
        schemaManager.getKeySchema(tableName, entityName).getRawSchema(),
        schemaManager.getEntitySchema(tableName, entityName).getRawSchema());
  }

  @Override
  public boolean delete(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists(String name) {
    String tableName = getTableName(name);
    String entityName = getEntityName(name);
    schemaManager.refreshManagedSchemaCache(tableName, entityName);
    return schemaManager.hasManagedSchema(tableName, entityName);
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

  /*
   * Use the partition strategy to extract the key fields so that we can create a key
   * schema.
   */
  static Schema getKeySchema(DatasetDescriptor descriptor) {
    Schema avroRecordSchema;
    if (HBaseDatasetRepository.isComposite(descriptor)) {
      // use first field schema for composite schemas
      avroRecordSchema = descriptor.getSchema().getFields().get(0).schema();
      if (avroRecordSchema.getType() == Schema.Type.UNION) {
        List<Schema> types = avroRecordSchema.getTypes();
        if (types.size() == 2) {
          if (types.get(0).getType() == Schema.Type.NULL) {
            avroRecordSchema = types.get(1);
          } else if (types.get(1).getType() == Schema.Type.NULL) {
            avroRecordSchema = types.get(0);
          }
        } else {
          throw new IllegalArgumentException("Unsupported union schema: " + avroRecordSchema);
        }
      }
    } else {
      avroRecordSchema = descriptor.getSchema();
    }
    Schema keySchema = Schema.createRecord(avroRecordSchema.getName(),
        "Key part of " + avroRecordSchema.getName(),
        avroRecordSchema.getNamespace(), false);
    List<Schema.Field> keyFields = Lists.newArrayList();
    List<String> keyFieldNames = Lists.newArrayList();
    for (FieldPartitioner fp : descriptor.getPartitionStrategy().getFieldPartitioners()) {
      keyFieldNames.add(fp.getName());
    }
    for (Schema.Field f : avroRecordSchema.getFields()) {
      if (keyFieldNames.contains(f.name())) {
        keyFields.add(copy(f));
      }
    }
    keySchema.setFields(keyFields);
    return keySchema;
  }

  private static Schema.Field copy(Schema.Field f) {
    Schema.Field copy = AvroUtils.cloneField(f);
    // retain mapping properties
    for (Map.Entry<String,JsonNode> prop : f.getJsonProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }
    return copy;
  }

  private static DatasetDescriptor getDatasetDescriptor(String keySchemaString,
      String entitySchemaString) {
    Schema keySchema = new Schema.Parser().parse(keySchemaString);
    PartitionStrategy.Builder partitionStrategyBuilder = new PartitionStrategy.Builder();
    for (Schema.Field f : keySchema.getFields()) {
      partitionStrategyBuilder.identity(f.name(), 1); // each key field is a partition field
    }
    return new DatasetDescriptor.Builder()
        .schema(entitySchemaString)
        .partitionStrategy(partitionStrategyBuilder.get())
        .get();
  }
}
