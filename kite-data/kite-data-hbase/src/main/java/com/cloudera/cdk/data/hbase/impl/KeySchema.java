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
package com.cloudera.cdk.data.hbase.impl;

import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.hbase.impl.EntitySchema.FieldMapping;

import java.util.Collection;

/**
 * The KeySchema type.
 */
public class KeySchema {

  private final String rawSchema;
  private final PartitionStrategy partitionStrategy;

  /**
   * @param rawSchema
   *          The raw schema
   */
  public KeySchema(String rawSchema, Collection<FieldMapping> fieldMappings) {
    this.rawSchema = rawSchema;
    PartitionStrategy.Builder strategyBuilder = new PartitionStrategy.Builder();
    for (FieldMapping fieldMapping : fieldMappings) {
      strategyBuilder.identity(fieldMapping.getFieldName(), String.class, 1);
    }
    partitionStrategy = strategyBuilder.build();
  }
  
  public KeySchema(String rawSchema, PartitionStrategy partitionStrategy) {
    this.rawSchema = rawSchema;
    this.partitionStrategy = partitionStrategy;
  }
  
  /**
   * Method meant to determine if two KeySchemas are compatible with each
   * other for schema migration purposes. Classes that inherit KeySchema
   * should override this implementation, since this implementation isn't able
   * to make that determination.
   * 
   * TODO: Figure out a base set of properties that all key schema
   * implementations should share in their implementation of determining
   * compatibility and execute that here.
   * 
   * @param keySchema
   *          The other KeySchema to determine compatible with
   * @return
   */
  public boolean compatible(KeySchema keySchema) {
    // throw an exception if anyone calls this directly, as this should be
    // overridden in derived classes.
    throw new UnsupportedOperationException(
        "KeyScheam class can't determine if two key schemas are compatible.");
  }

  /**
   * Get the raw schema
   * 
   * @return The raw schema.
   */
  public String getRawSchema() {
    return rawSchema;
  }

  public PartitionStrategy getPartitionStrategy() {
    return partitionStrategy;
  }
}
