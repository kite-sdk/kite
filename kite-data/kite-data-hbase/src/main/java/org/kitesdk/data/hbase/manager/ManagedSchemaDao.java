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

import java.util.List;

import org.kitesdk.data.hbase.manager.generated.ManagedSchema;

/**
 * Interface for fetching managed schemas from a persistence store. Managed
 * schemas are defined by the ManagedSchema avro record. Dao implementations
 * should be able to fetch these by key, defined by ManagedSchemaKey avro
 * record, from the persistence store.
 * 
 * ManagedSchemaKey is effectively a table name, entity name pair.
 */
interface ManagedSchemaDao {

  /**
   * Get all Managed Schemas from the persistence store.
   * 
   * @return The list of ManagedSchema instances
   */
  public List<ManagedSchema> getManagedSchemas();

  /**
   * Get a ManagedSchema for the table name, entity name pair. Returns null if
   * one doesn't exist.
   * 
   * @param tableName
   *          The table name of the managed schema we are fetching
   * @param entityName
   *          The entity name of the managed schema we are fetching
   * @return The ManagedSchema, or null if one doesn't exist.
   */
  public ManagedSchema getManagedSchema(String tableName, String entityName);

  /**
   * Save a Managed schema to the persistence store.
   * 
   * @param schema
   *          The ManagedSchema to save
   * @return True if the save succeeded, or false if there was a conflict
   */
  public boolean save(ManagedSchema schema);

  public boolean delete(ManagedSchema schema);
}
