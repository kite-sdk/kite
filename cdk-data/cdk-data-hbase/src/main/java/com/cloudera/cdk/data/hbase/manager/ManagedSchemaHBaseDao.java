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
package com.cloudera.cdk.data.hbase.manager;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;

import com.cloudera.cdk.data.dao.Dao;
import com.cloudera.cdk.data.dao.EntityScanner;
import com.cloudera.cdk.data.dao.KeyEntity;
import com.cloudera.cdk.data.hbase.avro.SpecificAvroDao;
import com.cloudera.cdk.data.hbase.avro.impl.AvroEntitySchema;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeyEntitySchemaParser;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeySchema;
import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
import com.cloudera.cdk.data.hbase.manager.generated.ManagedSchema;
import com.cloudera.cdk.data.hbase.manager.generated.ManagedSchemaKey;

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
