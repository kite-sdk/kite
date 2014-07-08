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
package org.kitesdk.data.hbase.manager;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;

import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.hbase.avro.AvroEntitySchema;
import org.kitesdk.data.hbase.avro.AvroKeyEntitySchemaParser;
import org.kitesdk.data.hbase.avro.AvroUtils;
import org.kitesdk.data.hbase.avro.SpecificAvroDao;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.EntityScanner;
import org.kitesdk.data.hbase.manager.generated.ManagedSchema;

/**
 * ManagedSchemaDao implementation backed by an HBase table. All of the managed
 * schemas are persisted to a table called "managed_schemas" unless otherwise
 * specified in the constructor.
 */
class ManagedSchemaHBaseDao implements ManagedSchemaDao {

  /**
   * The Default HBase table where this schema metadata is stored.
   */
  public static final String DEFAULT_MANAGED_SCHEMA_TABLE = "managed_schemas";

  /**
   * The schema parser we'll use to parse managed schemas.
   */
  private static final AvroKeyEntitySchemaParser schemaParser = new AvroKeyEntitySchemaParser();

  private static final AvroEntitySchema managedSchemaEntity;
  static {
    String str = AvroUtils.inputStreamToString(ManagedSchemaHBaseDao.class
        .getResourceAsStream("/ManagedSchema.avsc"));
    managedSchemaEntity = schemaParser.parseEntitySchema(str);
  }

  /**
   * An HBase Common DAO which is used to access the different entity schemas
   */
  private Dao<ManagedSchema> managedSchemaDao;

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
    managedSchemaDao = new SpecificAvroDao<ManagedSchema>(tablePool,
        managedSchemaTable, managedSchemaEntity.getRawSchema(),
        ManagedSchema.class);
  }

  @Override
  public List<ManagedSchema> getManagedSchemas() {
    List<ManagedSchema> returnList = new ArrayList<ManagedSchema>();
    EntityScanner<ManagedSchema> entityScanner = managedSchemaDao.getScanner();
    entityScanner.initialize();
    try {
      for (ManagedSchema entity : entityScanner) {
        returnList.add(entity);
      }
    } finally {
      entityScanner.close();
    }
    return returnList;
  }

  @Override
  public ManagedSchema getManagedSchema(String tableName, String entityName) {
    PartitionKey key = new PartitionKey(tableName, entityName);
    ManagedSchema managedSchemaRecord = managedSchemaDao.get(key);
    if (managedSchemaRecord == null) {
      return null;
    } else {
      return managedSchemaRecord;
    }
  }

  @Override
  public boolean save(ManagedSchema schema) {
    return managedSchemaDao.put(schema);
  }

  @Override
  public boolean delete(ManagedSchema schema) {
    return managedSchemaDao.delete(schema);
  }
}
