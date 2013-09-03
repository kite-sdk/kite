// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.manager;

import java.util.Map;

import com.cloudera.cdk.data.hbase.ConcurrentSchemaModificationException;
import com.cloudera.cdk.data.hbase.EntitySchema;
import com.cloudera.cdk.data.hbase.IncompatibleSchemaException;
import com.cloudera.cdk.data.hbase.KeySchema;
import com.cloudera.cdk.data.hbase.SchemaNotFoundException;
import com.cloudera.cdk.data.hbase.SchemaValidationException;

/**
 * Interface for managing schemas with a meta store. This interface provides
 * various methods for accessing the schema values in the meta store, as well as
 * methods for creating or migrating new schemas.
 */
public interface SchemaManager {

  /**
   * Returns true if this entity manager knows of a managed schema in the
   * specified table that goes by the entityName.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @return True if it has it, false otherwise
   */
  public boolean hasManagedSchema(String tableName, String entityName);

  /**
   * Get the key schema for managed schema.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @return The key schema.
   * @throws SchemaNotFoundException
   */
  public KeySchema getKeySchema(String tableName, String entityName);

  /**
   * Get the entity schema for the managed schema. This will return the entity
   * schema with the greatest version.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @return The entity schema
   * @throws SchemaNotFoundException
   */
  public EntitySchema getEntitySchema(String tableName, String entityName);

  /**
   * Get a specified version of the entity schema for the managed schema.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @param version
   *          The version of the entity schema.
   * @return The entity schema
   * @throws SchemaNotFoundException
   */
  public EntitySchema getEntitySchema(String tableName, String entityName,
      int version);

  /**
   * Get all entity schema versions for a managed schema identified by the table
   * name and entity name.
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   * @return The map of versioned entity schemas.
   */
  public Map<Integer, EntitySchema> getEntitySchemas(String tableName,
      String entityName);

  /**
   * Function takes an entity schema, and the table name and entity name of a
   * managed schema, and will return the version of that entity schema.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @param schema
   *          The entity schema, whose version this method will return.
   * @return The entity schema's version
   * @throws SchemaNotFoundException
   */
  public int getEntityVersion(String tableName, String entityName,
      EntitySchema schema);

  /**
   * Refresh the underlying ManagedSchema cache for a specific table name,
   * entity name pair. This will ensure that calls to get key or entity schemas
   * will return the latest data in the meta store.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   */
  public void refreshManagedSchemaCache(String tableName, String entityName);

  /**
   * Creates a new schema version. There must be no previous schema for this
   * table/entity name pair.
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   * @param keySchema
   *          The schema of the key being added
   * @param entitySchema
   *          The schema of the entity being added
   * @throws IncompatibleSchemaException
   *           if a previous schema exists
   * @throws ConcurrentSchemaModificationException
   * @throws SchemaValidationException
   */
  public void createSchema(String tableName, String entityName,
      String keySchemaStr, String entitySchemaStr, String schemaType,
      String keySerDeType, String entitySerDeType);

  /**
   * Creates a new schema version. The new schema must be an allowed schema
   * upgrade, which is dictated by the implementation.
   * 
   * @param tableName
   *          The table name of the managed schema
   * @param entityName
   *          The entity name of the managed schema
   * @param newSchema
   *          The new schema
   * @throws SchemaNotFoundException
   * @throws IncompatibleSchemaException
   * @throws ConcurrentSchemaModificationException
   * @throws SchemaValidationException
   */
  public void migrateSchema(String tableName, String entityName,
      String newSchema);
}
