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
package org.kitesdk.data.hbase.avro;

import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.hbase.impl.KeyEntitySchemaParser;

/**
 * This implementation parses the AvroKeySchema and AvroEntitySchema from Avro
 * schemas. The entities can contain metadata in annotations of the Avro record
 * and Avro record fields.
 * 
 * Each field must have a mapping annotation, which specifies how that field is
 * mapped to an HBase column.
 * 
 * Allowed mapping types are "key", "column", "keyAsColumn", and "occVersion".
 *
 * The key mapping type on a field indicates that there is an identity field
 * partitioner on the field. The identity field partitioners are taken in order.
 * 
 * The column mapping type on a field tells this entity mapper to map that field
 * to the fully_qualified_column.
 * 
 * The keyAsColumn mapping type on a field tells the entity mapper to map each
 * key of the value type to a column in the specified column_family. This
 * annotation is only allowed on map and record types.
 * 
 * The occVersion mapping type on a field indicates that the entity participates
 * in optimistic concurrency control, and the field is the version number that
 * is automatically incremented by the system to validate that there are no
 * write conflicts.
 * 
 * Here is an example schema:
 * 
 * <pre>
 * 
 * { 
 *   "name": "record_name",
 *   "type": "record",
 *   "partitions": [
 *     { "sourceName": "field1", "type": "identity" }
 *   ],
 *   "fields": [ 
 *     { 
 *       "name": "field1", 
 *       "type": "int", 
 *       "mapping": { "type": "column", "value": "meta:field1" } 
 *     },
 *      
 *     { 
 *       "name": "field2", 
 *       "type": { "type": "map", "values": "string" }, 
 *       "mapping": { "type": "keyAsColumn": "value": "map_family" } 
 *     }
 *   ]
 * }
 * 
 * </pre>
 * 
 * An Avro instance of this schema would have its field1 value encoded to the
 * meta:field1 column. Each key/value pair of the field2 map type would have its
 * value mapped to the map_family:[key] column. It will also participate in
 * optimistic concurrency control.
 */
public class AvroKeyEntitySchemaParser implements
    KeyEntitySchemaParser<AvroKeySchema, AvroEntitySchema> {

  @Override
  public AvroKeySchema parseKeySchema(String rawSchema) {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(rawSchema)
        .build();
    return new AvroKeySchema(
        descriptor.getSchema(), descriptor.getPartitionStrategy());
  }

  @Override
  public AvroKeySchema parseKeySchema(String rawSchema,
      PartitionStrategy partitionStrategy) {
    // use DatasetDescriptor.Builder because it checks consistency
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(rawSchema)
        .partitionStrategy(partitionStrategy)
        .build();
    return new AvroKeySchema(
        descriptor.getSchema(), descriptor.getPartitionStrategy());
  }

  @Override
  public AvroEntitySchema parseEntitySchema(String rawSchema) {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(rawSchema)
        .build();
    return new AvroEntitySchema(
        descriptor.getSchema(), rawSchema, descriptor.getColumnMapping());
  }

  @Override
  public AvroEntitySchema parseEntitySchema(String rawSchema,
      ColumnMapping columnMapping) {
    // use DatasetDescriptor.Builder because it checks consistency
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(rawSchema)
        .columnMapping(columnMapping)
        .build();
    return new AvroEntitySchema(
        descriptor.getSchema(), rawSchema, descriptor.getColumnMapping());
  }
}
