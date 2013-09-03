// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.manager;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;

import com.cloudera.cdk.data.hbase.Dao;
import com.cloudera.cdk.data.hbase.EntityMapper.KeyEntity;
import com.cloudera.cdk.data.hbase.EntityScanner;
import com.cloudera.cdk.data.hbase.avro.AvroEntitySchema;
import com.cloudera.cdk.data.hbase.avro.AvroKeyEntitySchemaParser;
import com.cloudera.cdk.data.hbase.avro.AvroKeySchema;
import com.cloudera.cdk.data.hbase.avro.AvroUtils;
import com.cloudera.cdk.data.hbase.avro.SpecificAvroDao;

/**
 * ManagedSchemaDao implementation backed by an HBase table. All of the managed
 * schemas are persisted to a table called "managed_schemas" unless otherwise
 * specified in the constructor.
 */
public class ManagedSchemaHBaseDao implements ManagedSchemaDao {

  /**
   * The Default HBase table where this schema metadata is stored.
   */
  public static final String DEFAULT_MANAGED_SCHEMA_TABLE = "managed_schemas";

  /**
   * The schema parser we'll use to parse managed schemas.
   */
  private static final AvroKeyEntitySchemaParser schemaParser = new AvroKeyEntitySchemaParser();

  private static final AvroEntitySchema managedSchemaEntity;
  private static final AvroKeySchema managedSchemaKey;
  static {
    managedSchemaEntity = schemaParser.parseEntity(AvroUtils
        .inputStreamToString(ManagedSchemaHBaseDao.class
            .getResourceAsStream("/ManagedSchema.avsc")));
    managedSchemaKey = schemaParser.parseKey(AvroUtils
        .inputStreamToString(ManagedSchemaHBaseDao.class
            .getResourceAsStream("/ManagedSchemaKey.avsc")));
  }

  /**
   * An HBase Common DAO which is used to access the different entity schemas
   */
  private Dao<ManagedSchemaKey, ManagedSchema> managedSchemaDao;

  /**
   * Constructor which uses the default managed schema table name, which is
   * managed_schemas.
   * 
   * @param tablePool
   *          The pool of HBase tables
   */
  public ManagedSchemaHBaseDao(HTablePool tablePool) {
    this(tablePool, DEFAULT_MANAGED_SCHEMA_TABLE);
  }

  public ManagedSchemaHBaseDao(HTablePool tablePool, String managedSchemaTable) {
    managedSchemaDao = new SpecificAvroDao<ManagedSchemaKey, ManagedSchema>(
        tablePool, managedSchemaTable, managedSchemaKey.getRawSchema(),
        managedSchemaEntity.getRawSchema(), ManagedSchemaKey.class,
        ManagedSchema.class);
  }

  @Override
  public List<ManagedKeySchemaPair> getManagedSchemas() {
    List<ManagedKeySchemaPair> returnList = new ArrayList<ManagedKeySchemaPair>();
    EntityScanner<ManagedSchemaKey, ManagedSchema> entityScanner = managedSchemaDao
        .getScanner();
    for (KeyEntity<ManagedSchemaKey, ManagedSchema> keyEntity : entityScanner) {
      returnList.add(new ManagedKeySchemaPair(keyEntity.getKey(), keyEntity
          .getEntity()));
    }
    return returnList;
  }

  @Override
  public ManagedKeySchemaPair getManagedSchema(String tableName,
      String entityName) {
    ManagedSchemaKey key = ManagedSchemaKey.newBuilder().setTable(tableName)
        .setName(entityName).build();
    ManagedSchema managedSchemaRecord = managedSchemaDao.get(key);
    if (managedSchemaRecord == null) {
      return null;
    } else {
      return new ManagedKeySchemaPair(key, managedSchemaRecord);
    }
  }

  @Override
  public boolean save(ManagedSchemaKey key, ManagedSchema schema) {
    return managedSchemaDao.put(key, schema);
  }
}
