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

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;

/**
 * The KeySchema type.
 */
public class KeySchema {

  private final String rawSchema;
  private final PartitionStrategy partitionStrategy;
  private final Map<String, Integer> fieldPositions;

  public KeySchema(String rawSchema, PartitionStrategy partitionStrategy) {
    this.rawSchema = rawSchema;
    this.partitionStrategy = partitionStrategy;
    this.fieldPositions = Maps.newHashMap();

    List<FieldPartitioner> partitioners =
        Accessor.getDefault().getFieldPartitioners(partitionStrategy);
    for (int i = 0; i < partitioners.size(); i += 1) {
      FieldPartitioner fp = partitioners.get(i);
      if (fp instanceof IdentityFieldPartitioner) {
        fieldPositions.put(fp.getSourceName(), i);
      }
    }
  }

  /**
   * Method meant to determine if two KeySchemas are compatible with each other
   * for schema migration purposes. Classes that inherit KeySchema should
   * override this implementation, since this implementation isn't able to make
   * that determination.
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
        "KeySchema class can't determine if two key schemas are compatible.");
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

  /**
   * Resolves the storage position of a field in keys with this KeySchema.
   *
   * @param fieldName
   *          The source field name.
   * @return
   *          The position of the field in keys with this schema.
   * @throws DatasetException
   *          If the field cannot be recovered from keys with this schema.
   */
  public int position(String fieldName) {
    if (fieldPositions.containsKey(fieldName)) {
      return fieldPositions.get(fieldName);
    } else {
      throw new DatasetException("Cannot recover " + fieldName + " from key");
    }
  }
}
