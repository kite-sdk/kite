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
import org.kitesdk.data.PartitionStrategy;

/**
 * Interface for the Kite HBase StorageKey and Entity parser.
 *
 * @param <KEY_SCHEMA>
 *          The type that the key schema gets parsed to. Extends
 *          KeySchema<RAW_SCHEMA>
 * @param <ENTITY_SCHEMA>
 *          The type that the raw entity schema gets parsed to. Extends
 *          EntitySchema<RAW_SCHEMA>
 */
public interface KeyEntitySchemaParser<KEY_SCHEMA extends KeySchema, ENTITY_SCHEMA extends EntitySchema> {

  /**
   * Parse the key schema from the schema string.
   *
   * @param schema
   *          The schema string
   * @return The KEY_SCHEMA
   */
  public KEY_SCHEMA parseKeySchema(String schema);

  /**
   * Parse the key schema from the schema string. Use the PartitionStrategy
   * passed instead of any field partitioner annotations on the schema.
   *
   * @param schema
   *          The schema string
   * @param partitionStrategy
   *          The partition strategy that defines the key.
   * @return The KEY_SCHEMA
   */
  public KEY_SCHEMA parseKeySchema(String schema,
      PartitionStrategy partitionStrategy);

  /**
   * Parse the raw entity schema.
   * 
   * @param schema
   *          The schema string
   * @return The ENTITY_SCHEMA
   */
  public ENTITY_SCHEMA parseEntitySchema(String schema);

  /**
   * Parse the entity schema. Use the ColumnMapping passed instead of
   * any field mapping annotations on the schema.
   *
   * @param schema
   *          The schema string
   * @param mappingDescriptor
   *          The ColumnMapping that defines the column mappings for
   *          this entity.
   * @return The ENTITY_SCHEMA
   */
  public ENTITY_SCHEMA parseEntitySchema(String schema,
      ColumnMapping mappingDescriptor);
}
