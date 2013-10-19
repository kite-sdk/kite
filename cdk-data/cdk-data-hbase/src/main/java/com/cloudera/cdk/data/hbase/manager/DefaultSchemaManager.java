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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.dao.ConcurrentSchemaModificationException;
import com.cloudera.cdk.data.dao.EntitySchema;
import com.cloudera.cdk.data.dao.HBaseCommonException;
import com.cloudera.cdk.data.dao.IncompatibleSchemaException;
import com.cloudera.cdk.data.dao.KeySchema;
import com.cloudera.cdk.data.dao.SchemaManager;
import com.cloudera.cdk.data.dao.SchemaNotFoundException;
import com.cloudera.cdk.data.hbase.KeyEntitySchemaParser;
import com.cloudera.cdk.data.hbase.manager.generated.ManagedSchema;
import com.google.common.collect.Lists;

/**
 * The Default SchemaManager implementation. It uses a ManagedSchemaDao
 * implementation, passed in the constructor, to read schema metadata, and
 * persist schema metadata for schema creations and migrations.
 */
public class DefaultSchemaManager implements SchemaManager {

  private static Logger LOG = LoggerFactory.getLogger(DefaultSchemaManager.class);

  /**
   * A mapping of managed schema row keys to the managed schema entities.
   */
  private volatile ConcurrentHashMap<String, ManagedSchema> managedSchemaMap;

  /**
   * A DAO which is used to access the managed key and entity schemas
   */
  private ManagedSchemaDao managedSchemaDao;

  /**
   * A map of schema parsers that can be used to parse different schema types.
   */
  private ConcurrentHashMap<String, KeyEntitySchemaParser<?, ?>> schemaParsers = new ConcurrentHashMap<String, KeyEntitySchemaParser<?, ?>>();

  /**
   * Constructor which uses the default managed schema table name, which is
   * managed_schemas.
   * 
   * @param tablePool
   *          The pool of HBase tables
   */
  public DefaultSchemaManager(HTablePool tablePool) {
    this(new ManagedSchemaHBaseDao(tablePool));
  }

  public DefaultSchemaManager(HTablePool tablePool, String managedSchemaTable) {
    this(new ManagedSchemaHBaseDao(tablePool, managedSchemaTable));
  }

  public DefaultSchemaManager(ManagedSchemaDao managedSchemaDao) {
    this.managedSchemaDao = managedSchemaDao;
  }

  @Override
  public boolean hasManagedSchema(String tableName, String entityName) {
    ManagedSchema managedSchema;
    try {
      managedSchema = getManagedSchema(tableName, entityName);
    } catch (SchemaNotFoundException e) {
      managedSchema = null;
    }
    return managedSchema != null;
  }

  @Override
  public KeySchema getKeySchema(String tableName, String entityName) {
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());
    String greatestVersionedSchema = getGreatestEntitySchemaString(managedSchema);
    return schemaParser.parseKeySchema(greatestVersionedSchema);
  }

  @Override
  public EntitySchema getEntitySchema(String tableName, String entityName) {
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());
    String greatestVersionedSchema = getGreatestEntitySchemaString(managedSchema);
    return schemaParser.parseEntitySchema(greatestVersionedSchema);
  }
  
  private String getGreatestEntitySchemaString(ManagedSchema managedSchema) {
    int greatestVersion = -1;
    String greatestVersionedSchema = null;
    for (Entry<String, String> entry : managedSchema.getEntitySchemas()
        .entrySet()) {
      int version = Integer.parseInt(entry.getKey());
      if (version > greatestVersion) {
        greatestVersion = version;
        greatestVersionedSchema = entry.getValue();
      }
    }

    if (greatestVersionedSchema == null) {
      String msg = "No schema versions for " + managedSchema.getTable() + ", " + managedSchema.getName();
      LOG.error(msg);
      throw new SchemaNotFoundException(msg);
    }
    
    return greatestVersionedSchema;
  }

  @Override
  public EntitySchema getEntitySchema(String tableName, String entityName,
      int version) {
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());
    if (!managedSchema.getEntitySchemas().containsKey(String.valueOf(version))) {
      // didn't contain the schema version, refresh the schema cache and refetch
      // the managed schema.
      refreshManagedSchemaCache(tableName, entityName);
      managedSchema = getManagedSchema(tableName, entityName);
    }
    String schema = managedSchema.getEntitySchemas().get(
        String.valueOf(version));
    if (schema != null) {
      return schemaParser.parseEntitySchema(schema);
    } else {
      String msg = "Could not find managed schema for " + tableName + ", "
          + entityName + ", and version " + Integer.toString(version);
      LOG.error(msg);
      throw new SchemaNotFoundException(msg);
    }
  }

  @Override
  public Map<Integer, EntitySchema> getEntitySchemas(String tableName,
      String entityName) {
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());
    Map<Integer, EntitySchema> retMap = new HashMap<Integer, EntitySchema>();
    for (Entry<String, String> entry : managedSchema.getEntitySchemas()
        .entrySet()) {
      EntitySchema entitySchema = schemaParser.parseEntitySchema(entry.getValue());
      retMap.put(Integer.parseInt(entry.getKey()), entitySchema);
    }
    return retMap;
  }

  @Override
  public int getEntityVersion(String tableName, String entityName,
      EntitySchema schema) {
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(getManagedSchema(
        tableName, entityName).getSchemaType());
    for (Entry<Integer, String> entry : getManagedSchemaVersions(tableName,
        entityName).entrySet()) {
      EntitySchema managedSchema = schemaParser.parseEntitySchema(entry.getValue());
      if (schema.equals(managedSchema)) {
        return entry.getKey();
      }
    }
    return -1;
  }

  @Override
  public void createSchema(String tableName, String entityName,
      String entitySchemaStr, String schemaParserType,
      String keySerDeType, String entitySerDeType) {

    // We want to make sure the managed schema map has as recent
    // a copy of the managed schema in HBase as possible.
    refreshManagedSchemaCache(tableName, entityName);

    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(schemaParserType);
    KeySchema keySchema = schemaParser.parseKeySchema(entitySchemaStr);
    EntitySchema entitySchema = schemaParser.parseEntitySchema(entitySchemaStr);

    try {
      ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
      if (managedSchema != null) {
        throw new IncompatibleSchemaException(
            "Cannot create schema when one already exists");
      }
    } catch (SchemaNotFoundException e) {
      // we want the schema to not be found, continue
    }

    ManagedSchema managedSchema = ManagedSchema.newBuilder()
        .setName(entityName).setTable(tableName)
        .setEntitySchemas(new HashMap<String, String>())
        .setSchemaType(schemaParserType).setEntitySerDeType(entitySerDeType)
        .setKeySerDeType(keySerDeType).build();

    // at this point, the schema is a valid migration. persist it.
    managedSchema.getEntitySchemas().put("0", entitySchema.getRawSchema());
    if (!managedSchemaDao.save(managedSchema)) {
      throw new ConcurrentSchemaModificationException(
          "The schema has been updated concurrently.");
    }
    getManagedSchemaMap().put(getManagedSchemaMapKey(managedSchema.getTable(), managedSchema.getName()), managedSchema);
  }

  @Override
  public void migrateSchema(String tableName, String entityName,
      String newSchemaStr) {
    // We want to make sure the managed schema map has as recent
    // a copy of the managed schema in HBase as possible.
    refreshManagedSchemaCache(tableName, entityName);

    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());

    // validate it's a valid avro schema by parsing it
    EntitySchema newEntitySchema = schemaParser.parseEntitySchema(newSchemaStr);
    KeySchema newKeySchema = schemaParser.parseKeySchema(newSchemaStr);

    // verify that the newSchema isn't a duplicate of a previous schema version.
    int existingVersion = getEntityVersion(tableName, entityName, newEntitySchema);
    if (existingVersion != -1) {
      throw new IncompatibleSchemaException(
          "Schema already exists as version: "
              + Integer.toString(existingVersion));
    }

    // validate that, for each version of the schema, this schema is
    // compatible with those schema version. That means the field mapping
    // hasn't changed, and we can read old schemas, and processes that
    // are configured with old schemas can read new schemas.
    int greatestSchemaVersion = 0;
    for (Entry<String, String> entry : managedSchema.getEntitySchemas()
        .entrySet()) {
      int version = Integer.parseInt(entry.getKey());
      if (version > greatestSchemaVersion) {
        greatestSchemaVersion = version;
      }
      String schemaString = entry.getValue();
      KeySchema keySchema = schemaParser.parseKeySchema(schemaString);
      EntitySchema entitySchema = schemaParser.parseEntitySchema(schemaString);
      if (!newKeySchema.compatible(keySchema)) {
        String msg = "Key fields of entity schema not compatible with version "
            + Integer.toString(version) + ": Old schema: " + schemaString
            + " New schema: " + newEntitySchema.getRawSchema();
        throw new IncompatibleSchemaException(msg);
      }
      if (!newEntitySchema.compatible(entitySchema)) {
        String msg = "Avro schema not compatible with version "
            + Integer.toString(version) + ": Old schema: " + schemaString
            + " New schema: " + newEntitySchema.getRawSchema();
        throw new IncompatibleSchemaException(msg);
      }
    }

    // at this point, the schema is a valid migration. persist it.
    managedSchema.getEntitySchemas().put(
        Integer.toString(greatestSchemaVersion + 1), newEntitySchema.getRawSchema());
    if (!managedSchemaDao.save(managedSchema)) {
      throw new ConcurrentSchemaModificationException(
          "The schema has been updated concurrently.");
    }
  }

  @Override
  public void deleteSchema(String tableName, String entityName) {
    // We want to make sure the managed schema map has as recent
    // a copy of the managed schema in HBase as possible.
    refreshManagedSchemaCache(tableName, entityName);

    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);

    if (!managedSchemaDao.delete(managedSchema)) {
      throw new ConcurrentSchemaModificationException(
          "The schema has been updated concurrently.");
    }

    getManagedSchemaMap().remove(getManagedSchemaMapKey(managedSchema.getTable(), managedSchema.getName()));
  }

  /**
   * Update the managedSchemaMap for the entry defined by tableName and
   * entityName.
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   */
  @Override
  public void refreshManagedSchemaCache(String tableName, String entityName) {
    ManagedSchema managedSchema = managedSchemaDao.getManagedSchema(
        tableName, entityName);
    if (managedSchema != null) {
      getManagedSchemaMap().put(getManagedSchemaMapKey(managedSchema.getTable(), managedSchema.getName()),
          managedSchema);
    }
  }

  @Override
  public List<String> getEntityNames(String tableName) {
    List<String> names = Lists.newArrayList();
    for (ManagedSchema managedSchema : managedSchemaMap.values()) {
      if (managedSchema.getTable().equals(tableName)) {
        names.add(managedSchema.getName());
      }
    }
    return names;
  }

  /**
   * Get the managedSchemaMap, lazily loading it if it hasn't been initialized
   * yet. Members of this class should never access the managedSchemaMap
   * directly, but should always access it through this method.
   * 
   * @return The managedSchemaMap
   */
  private ConcurrentHashMap<String, ManagedSchema> getManagedSchemaMap() {
    if (managedSchemaMap == null) {
      synchronized (this) {
        if (managedSchemaMap == null) {
          managedSchemaMap = new ConcurrentHashMap<String, ManagedSchema>();
          populateManagedSchemaMap();
        }
      }
    }
    return managedSchemaMap;
  }

  /**
   * Populate the managedSchemaMap with all of the managed schemas returned by
   * the managedSchemaDao.
   */
  private void populateManagedSchemaMap() {
    for (ManagedSchema managedSchema : managedSchemaDao.getManagedSchemas()) {
      getManagedSchemaMap().put(getManagedSchemaMapKey(managedSchema.getTable(), managedSchema.getName()),
          managedSchema);
    }
  }

  /**
   * Get the managed schema from the managedSchemaMap, using the
   * getManagedSchemaMap() accessor.
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   * @return The ManagedSchema instance, or null if one doesn't exist.
   */
  private ManagedSchema getManagedSchemaFromSchemaMap(String tableName,
      String entityName) {
    return getManagedSchemaMap().get(getManagedSchemaMapKey(tableName, entityName));
  }

  /**
   * 
   * @param schemaParserClassName
   * @return
   */
  @SuppressWarnings("unchecked")
  private KeyEntitySchemaParser<?, ?> getSchemaParser(
      String schemaParserClassName) {
    if (schemaParsers.contains(schemaParserClassName)) {
      return schemaParsers.get(schemaParserClassName);
    } else {
      try {
        Class<KeyEntitySchemaParser<?, ?>> schemaParserClass = (Class<KeyEntitySchemaParser<?, ?>>) Class
            .forName(schemaParserClassName);
        KeyEntitySchemaParser<?, ?> schemaParser = schemaParserClass
            .getConstructor().newInstance();
        schemaParsers.putIfAbsent(schemaParserClassName, schemaParser);
        return schemaParser;
      } catch (Exception e) {
        throw new HBaseCommonException(
            "Could not instantiate schema parser class: "
                + schemaParserClassName, e);
      }
    }
  }

  /**
   * Get a map of schema versions for a managed schemas.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @return The managed schema version map.
   * @throws SchemaNotFoundException
   *           Thrown if a managed schema can't be found for the table name
   *           entity name.
   */
  private Map<Integer, String> getManagedSchemaVersions(String tableName,
      String entityName) {

    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    Map<Integer, String> returnMap = new HashMap<Integer, String>();
    for (Map.Entry<String, String> versionsEntry : managedSchema
        .getEntitySchemas().entrySet()) {
      returnMap.put(Integer.parseInt(versionsEntry.getKey()),
          versionsEntry.getValue());
    }
    return returnMap;
  }

  /**
   * Get the ManagedSchema entity for the tableName, entityName managed schema.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @return The ManagedSchema entity
   * @throws SchemaNotFoundException
   */
  private ManagedSchema getManagedSchema(String tableName, String entityName) {
    ManagedSchema managedSchema = getManagedSchemaFromSchemaMap(tableName,
        entityName);
    if (managedSchema == null) {
      refreshManagedSchemaCache(tableName, entityName);
      managedSchema = getManagedSchemaFromSchemaMap(tableName, entityName);
      if (managedSchema == null) {
        String msg = "Could not find managed schemas for " + tableName + ", "
            + entityName;
        throw new SchemaNotFoundException(msg);
      }
    }
    return managedSchema;
  }
  
  private String getManagedSchemaMapKey(String tableName, String entityName) {
    return tableName + ":" + entityName;
  }
}
