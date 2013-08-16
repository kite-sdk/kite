// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

import java.util.List;

/**
 * Interface for fetching managed schemas from a persistence store. Managed
 * schemas are defined by the ManagedSchema avro record. Dao implementations
 * should be able to fetch these by key, defined by ManagedSchemaKey avro
 * record, from the persistence store.
 * 
 * ManagedSchemaKey is effectively a table name, entity name pair.
 */
public interface AvroManagedSchemaDao {

  /**
   * Encapsulates a ManagedSchemaKey, and ManagedSchema. This is returned from
   * rows of the persistence store.
   */
  public class ManagedKeySchemaPair {

    private final ManagedSchemaKey key;
    private final ManagedSchema managedSchema;

    public ManagedKeySchemaPair(ManagedSchemaKey key,
        ManagedSchema managedSchema) {
      this.key = key;
      this.managedSchema = managedSchema;
    }

    public ManagedSchemaKey getKey() {
      return key;
    }

    public ManagedSchema getManagedSchema() {
      return managedSchema;
    }
  }

  /**
   * Get all Managed Schemas from the persistence store.
   * 
   * @return The list of ManagedKeySchemaPair instances
   */
  public List<ManagedKeySchemaPair> getManagedSchemas();

  /**
   * Get a ManagedSchema for the table name, entity name pair. Returns null if
   * one doesn't exist.
   * 
   * @param tableName
   *          The table name of the managed schema we are fetching
   * @param entityName
   *          The entity name of the managed schema we are fetching
   * @return The ManagedKeySchemaPair, or null if one doesn't exist.
   */
  public ManagedKeySchemaPair getManagedSchema(String tableName,
      String entityName);

  /**
   * Save a Managed schema to the persistence store.
   * 
   * @param key
   *          The key (table name, entity name) of the ManagedSchema
   * @param schema
   *          The ManagedSchema to save
   * @return True if the save succeeded, or false if there wwas a conflict
   */
  public boolean save(ManagedSchemaKey key, ManagedSchema schema);
}
