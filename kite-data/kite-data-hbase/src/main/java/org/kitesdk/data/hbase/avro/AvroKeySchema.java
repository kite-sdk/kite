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

import java.util.List;
import org.apache.avro.Schema;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.hbase.impl.KeySchema;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.SchemaUtil;

/**
 * A KeySchema implementation powered by Avro.
 */
public class AvroKeySchema extends KeySchema {

  private final Schema schema;

  public AvroKeySchema(Schema entitySchema,
      PartitionStrategy partitionStrategy) {
    super(null, partitionStrategy);
    this.schema = SchemaUtil.keySchema(entitySchema, partitionStrategy);
  }

  @Override
  public boolean compatible(KeySchema keySchema) {
    List<FieldPartitioner> fieldPartitioners = Accessor.getDefault()
        .getFieldPartitioners(getPartitionStrategy());
    List<FieldPartitioner> otherFieldPartitioners = Accessor.getDefault()
        .getFieldPartitioners(keySchema.getPartitionStrategy());
    if (fieldPartitioners.size() != otherFieldPartitioners.size()) {
      return false;
    }
    return AvroUtils.avroSchemaTypesEqual(schema, ((AvroKeySchema)keySchema).schema);
  }

  @Override
  public String getRawSchema() {
    return getAvroSchema().toString();
  }

  /**
   * Get the Avro Schema that underlies this KeySchema implementation.
   * 
   * @return The Avro Schema
   */
  public Schema getAvroSchema() {
    return schema;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    AvroKeySchema other = (AvroKeySchema) obj;
    if (schema == null) {
      if (other.schema != null)
        return false;
    } else if (!schema.equals(other.schema))
      return false;
    return true;
  }
}
