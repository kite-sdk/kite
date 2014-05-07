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
package org.kitesdk.data.hbase.impl;

import org.kitesdk.data.ColumnMapping;

/**
 * An EntitySchema is the parsed schema that contains the properties of an HBase
 * Common entity schema.
 */
public class EntitySchema {

  private final ColumnMapping mapping;
  private final String rawSchema;
  private final String name;

  /**
   * Constructs the EntitySchema
   * 
   * @param name
   *          The name of the entity schema
   * @param rawSchema
   *          The raw schema type that underlies the EntitySchema implementation
   * @param columnMapping
   *          The list of FieldMappings that specify how each field maps to an
   *          HBase row
   */
  public EntitySchema(String name, String rawSchema, ColumnMapping columnMapping) {
    this.name = name;
    this.rawSchema = rawSchema;
    this.mapping = columnMapping;
  }

  /**
   * Get the name of this EntitySchema
   * 
   * @return The name
   */
  public String getName() {
    return name;
  }

  public ColumnMapping getColumnMappingDescriptor() {
    return mapping;
  }

  /**
   * Get the raw schema that was parsed to create this schema.
   * 
   * @return The raw scheam.
   */
  public String getRawSchema() {
    return rawSchema;
  }

  /**
   * Method meant to determine if two EntitySchemas are compatible with each
   * other for schema migration purposes. Classes that inherit EntitySchema
   * should override this implementation, since this implemetnation isn't able
   * to make that determination.
   * 
   * TODO: Figure out a base set of properties that all entity schema
   * implementations should share in their implementation of determining
   * compatibility and execute that here.
   * 
   * @param entitySchema
   *          The other EntitySchema to determine compatible with
   * @return
   */
  public boolean compatible(EntitySchema entitySchema) {
    // throw an exception if anyone calls this directly, as this should be
    // overridden in derived classes.
    throw new UnsupportedOperationException(
        "EntityScheam class can't determine if two entity schemas are compatible.");
  }
}
