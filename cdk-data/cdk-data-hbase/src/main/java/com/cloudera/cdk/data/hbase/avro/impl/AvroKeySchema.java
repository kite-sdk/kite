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
package com.cloudera.cdk.data.hbase.avro.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.dao.EntitySchema.FieldMapping;
import com.cloudera.cdk.data.dao.KeySchema;

/**
 * A KeySchema implementation powered by Avro.
 */
public class AvroKeySchema extends KeySchema {

  private final Schema schema;

  /**
   * Constructor for the AvroKeySchema.
   * 
   * @param schema
   *          The Avro Schema that underlies this KeySchema implementation
   * @param rawSchema
   *          The Avro Schema as a string that underlies the KeySchema
   *          implementation
   */
  public AvroKeySchema(Schema schema, String rawSchema,
      List<FieldMapping> keyFieldMappings) {
    super(rawSchema, keyFieldMappings);
    List<Field> fieldsPartOfKey = new ArrayList<Field>();
    for (Field field : schema.getFields()) {
      for (FieldMapping fieldMapping : keyFieldMappings) {
        if (field.name().equals(fieldMapping.getFieldName())) {
          fieldsPartOfKey.add(AvroUtils.cloneField(field));
        }
      }
    }
    this.schema = Schema.createRecord(fieldsPartOfKey);
  }

  public AvroKeySchema(Schema schema, String rawSchema,
      PartitionStrategy partitionStrategy) {
    super(rawSchema, partitionStrategy);
    this.schema = schema;
  }

  @Override
  public boolean compatible(KeySchema keySchema) {
    List<FieldPartitioner> fieldPartitioners = getPartitionStrategy()
        .getFieldPartitioners();
    List<FieldPartitioner> otherFieldPartitioners = keySchema
        .getPartitionStrategy().getFieldPartitioners();
    if (fieldPartitioners.size() != otherFieldPartitioners.size()) {
      return false;
    }
    return AvroUtils.avroSchemaTypesEqual(schema, ((AvroKeySchema)keySchema).schema);
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
