// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * Interface for managing managed schemas. A managed schema is a pair of key
 * schema, and entity schema. Managed schemas are keyed on table name, and
 * entity name. Multiple versions of the entity schema can exist, though they
 * must be compatible by the schema resolution specification as stated in the
 * Avro spec.
 * 
 * Implementations can manage these schemas in a persistent store, like an RDBMS
 * or HBase itself for example.
 * 
 * @param <RAW_SCHEMA>
 *          The raw schema type. This is the type that the HBaseAvroSchemaParser
 *          parses to convert to an HBaseCommon key or entity schema.
 * @param <KEY_SCHEMA>
 *          The key schema type
 * @param <ENTITY_SCHEMA>
 *          The entity schema type
 */
public interface EntityManager<RAW_SCHEMA, KEY_SCHEMA extends KeySchema<RAW_SCHEMA>, ENTITY_SCHEMA extends EntitySchema<RAW_SCHEMA>> {

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
  public KEY_SCHEMA getKeySchema(String tableName, String entityName);

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
  public ENTITY_SCHEMA getEntitySchema(String tableName, String entityName);

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
  public ENTITY_SCHEMA getEntitySchema(String tableName, String entityName,
      int version);

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
      ENTITY_SCHEMA schema);

  /**
   * Constructs an entity mapper that is able to map to and from entities
   * defined by the managed schema.
   * 
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @return The entity mapper
   * @throws SchemaNotFoundException
   */
  public <K, E> EntityMapper<K, E> createEntityMapper(String tableName,
      String entityName);

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
      RAW_SCHEMA keySchema, RAW_SCHEMA entitySchema);

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
      RAW_SCHEMA newSchema);
}
