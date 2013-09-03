// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.manager;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTablePool;

import com.cloudera.cdk.data.hbase.ConcurrentSchemaModificationException;
import com.cloudera.cdk.data.hbase.EntitySchema;
import com.cloudera.cdk.data.hbase.HBaseCommonException;
import com.cloudera.cdk.data.hbase.IncompatibleSchemaException;
import com.cloudera.cdk.data.hbase.KeyEntitySchemaParser;
import com.cloudera.cdk.data.hbase.KeySchema;
import com.cloudera.cdk.data.hbase.SchemaNotFoundException;
import com.cloudera.cdk.data.hbase.manager.ManagedSchemaDao.ManagedKeySchemaPair;

/**
 * The Default SchemaManager implementation. It uses a ManagedSchemaDao
 * implementation, passed in the constructor, to read schema metadata, and
 * persist schema metadata for schema creations and migrations.
 */
public class DefaultSchemaManager implements SchemaManager {

  private static Log LOG = LogFactory.getLog(DefaultSchemaManager.class);

  /**
   * A mapping of managed schema row keys to the managed schema entities.
   */
  private volatile ConcurrentHashMap<ManagedSchemaKey, ManagedSchema> managedSchemaMap;

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

    return schemaParser.parseKey(managedSchema.getKeySchema());
  }

  @Override
  public EntitySchema getEntitySchema(String tableName, String entityName) {
    ManagedSchema managedSchema = getManagedSchema(tableName, entityName);
    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(managedSchema
        .getSchemaType());

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
      String msg = "No schema versions for " + tableName + ", " + entityName;
      LOG.error(msg);
      throw new SchemaNotFoundException(msg);
    }
    return schemaParser.parseEntity(greatestVersionedSchema);
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
      return schemaParser.parseEntity(schema);
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
      EntitySchema entitySchema = schemaParser.parseEntity(entry.getValue());
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
      EntitySchema managedSchema = schemaParser.parseEntity(entry.getValue());
      if (schema.equals(managedSchema)) {
        return entry.getKey();
      }
    }
    return -1;
  }

  @Override
  public void createSchema(String tableName, String entityName,
      String keySchemaStr, String entitySchemaStr, String schemaParserType,
      String keySerDeType, String entitySerDeType) {

    // We want to make sure the managed schema map has as recent
    // a copy of the managed schema in HBase as possible.
    refreshManagedSchemaCache(tableName, entityName);

    KeyEntitySchemaParser<?, ?> schemaParser = getSchemaParser(schemaParserType);
    KeySchema keySchema = schemaParser.parseKey(keySchemaStr);
    EntitySchema entitySchema = schemaParser.parseEntity(entitySchemaStr);

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
        .setKeySchema(keySchema.getRawSchema())
        .setEntitySchemas(new HashMap<String, String>())
        .setSchemaType(schemaParserType).setEntitySerDeType(entitySerDeType)
        .setKeySerDeType(keySerDeType).build();

    // at this point, the schema is a valid migration. persist it.
    managedSchema.getEntitySchemas().put("0", entitySchema.getRawSchema());
    ManagedSchemaKey key = ManagedSchemaKey.newBuilder().setTable(tableName)
        .setName(entityName).build();
    if (!managedSchemaDao.save(key, managedSchema)) {
      throw new ConcurrentSchemaModificationException(
          "The schema has been updated concurrently.");
    }
    getManagedSchemaMap().put(key, managedSchema);
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
    EntitySchema newSchema = schemaParser.parseEntity(newSchemaStr);

    // verify that the newSchema isn't a duplicate of a previous schema version.
    int existingVersion = getEntityVersion(tableName, entityName, newSchema);
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
      if (!newSchema.compatible(schemaParser.parseEntity(schemaString))) {
        String msg = "Avro schema not compatible with version "
            + Integer.toString(version) + ": Old schema: " + schemaString
            + " New schema: " + newSchema.getRawSchema();
        throw new IncompatibleSchemaException(msg);
      }
    }

    // at this point, the schema is a valid migration. persist it.
    managedSchema.getEntitySchemas().put(
        Integer.toString(greatestSchemaVersion + 1), newSchema.getRawSchema());
    ManagedSchemaKey key = ManagedSchemaKey.newBuilder().setTable(tableName)
        .setName(entityName).build();
    if (!managedSchemaDao.save(key, managedSchema)) {
      throw new ConcurrentSchemaModificationException(
          "The schema has been updated concurrently.");
    }
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
    ManagedKeySchemaPair keyEntity = managedSchemaDao.getManagedSchema(
        tableName, entityName);
    if (keyEntity != null) {
      getManagedSchemaMap().put(keyEntity.getKey(),
          keyEntity.getManagedSchema());
    }
  }

  /**
   * Get the managedSchemaMap, lazily loading it if it hasn't been initialized
   * yet. Members of this class should never access the managedSchemaMap
   * directly, but should always access it through this method.
   * 
   * @return The managedSchemaMap
   */
  private ConcurrentHashMap<ManagedSchemaKey, ManagedSchema> getManagedSchemaMap() {
    if (managedSchemaMap == null) {
      synchronized (this) {
        if (managedSchemaMap == null) {
          managedSchemaMap = new ConcurrentHashMap<ManagedSchemaKey, ManagedSchema>();
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
    for (ManagedKeySchemaPair keyEntity : managedSchemaDao.getManagedSchemas()) {
      getManagedSchemaMap().put(keyEntity.getKey(),
          keyEntity.getManagedSchema());
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
  private ManagedSchema getManagedSchemaFromSchemaMap1(String tableName,
      String entityName) {
    ManagedSchemaKey key = ManagedSchemaKey.newBuilder().setTable(tableName)
        .setName(entityName).build();
    return getManagedSchemaMap().get(key);
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
    ManagedSchema managedSchema = getManagedSchemaFromSchemaMap1(tableName,
        entityName);
    if (managedSchema == null) {
      refreshManagedSchemaCache(tableName, entityName);
      managedSchema = getManagedSchemaFromSchemaMap1(tableName, entityName);
      if (managedSchema == null) {
        String msg = "Could not find managed schemas for " + tableName + ", "
            + entityName;
        throw new SchemaNotFoundException(msg);
      }
    }
    return managedSchema;
  }
}
