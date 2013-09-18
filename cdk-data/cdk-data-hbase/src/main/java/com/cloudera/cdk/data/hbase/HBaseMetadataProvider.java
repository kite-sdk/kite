package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.DatasetDescriptor;
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

  public static final String ENTITY_NAME = "default"; // only support one entity per table
  private HBaseAdmin hbaseAdmin;
  private SchemaManager schemaManager;

  public HBaseMetadataProvider(HBaseAdmin hbaseAdmin, HTablePool tablePool) {
    this.hbaseAdmin = hbaseAdmin;
    this.schemaManager = new DefaultSchemaManager(tablePool);
  }

  @Override
  public DatasetDescriptor create(String tableName, DatasetDescriptor descriptor) {

    String keySchemaString = getKeySchema(descriptor).toString(true);
    String entitySchemaString = descriptor.getSchema().toString(true);

    AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
    AvroEntitySchema entitySchema = parser.parseEntity(entitySchemaString);

    schemaManager.createSchema(tableName, ENTITY_NAME, keySchemaString,
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
    schemaManager.migrateSchema(name, ENTITY_NAME, descriptor.getSchema().toString());
    return descriptor;
  }

  @Override
  public DatasetDescriptor load(String name) {
    if (!exists(name)) {
      throw new NoSuchDatasetException("No such dataset: " + name);
    }

    return getDatasetDescriptor(
        schemaManager.getKeySchema(name, ENTITY_NAME).getRawSchema(),
        schemaManager.getEntitySchema(name, ENTITY_NAME).getRawSchema());
  }

  @Override
  public boolean delete(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists(String name) {
    return schemaManager.hasManagedSchema(name, ENTITY_NAME);
  }

  /*
   * Use the partition strategy to extract the key fields so that we can create a key
   * schema.
   */
  static Schema getKeySchema(DatasetDescriptor descriptor) {
    Schema avroRecordSchema = descriptor.getSchema();
    Schema keySchema = Schema.createRecord(avroRecordSchema.getName() + "Key",
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
