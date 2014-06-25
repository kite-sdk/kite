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

import com.google.common.base.Objects;


import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.hbase.impl.EntitySchema;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.spi.SchemaUtil;
import org.kitesdk.data.spi.SchemaValidationUtil;

/**
 * An EntitySchema implementation powered by Avro.
 */
public class AvroEntitySchema extends EntitySchema {

  private final Schema schema;

  /**
   * Constructor for the AvroEntitySchema.
   * 
   * @param schema
   *          The Avro Schema that underlies this EntitySchema implementation
   * @param rawSchema
   *          The Avro Schema as a string that underlies the EntitySchema
   *          implementation
   * @param columnMapping
   *          The list of FieldMappings that specify how each field maps to an
   *          HBase row
   */
  public AvroEntitySchema(Schema schema, String rawSchema,
      ColumnMapping columnMapping) {
    super(schema.getName(), rawSchema, columnMapping);
    this.schema = schema;
  }

  /**
   * Get the Avro Schema that underlies this EntitySchema implementation.
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
    AvroEntitySchema other = (AvroEntitySchema) obj;
    if (schema == null) {
      if (other.schema != null)
        return false;
    } else {
      if (schema.getFields().size() != other.getAvroSchema().getFields().size()) {
        return false;
      }
      for (Field field : schema.getFields()) {
        Field entitySchemaField = other.getAvroSchema().getFields()
            .get(field.pos());
        if (!fieldsEqual(
            field,
            this.getColumnMappingDescriptor().getFieldMapping(field.name()),
            entitySchemaField,
            other.getColumnMappingDescriptor().getFieldMapping(
                entitySchemaField.name()))) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean compatible(EntitySchema entitySchema) {
    if (!mappingCompatible(this, entitySchema)) {
      return false;
    }
    AvroEntitySchema avroEntitySchema = (AvroEntitySchema) entitySchema;
    if (!SchemaValidationUtil.canRead(schema,
        avroEntitySchema.getAvroSchema())) {
      return false;
    }
    if (!SchemaValidationUtil.canRead(avroEntitySchema.getAvroSchema(),
        schema)) {
      return false;
    }
    return true;
  }

  /**
   * Ensure that the column mappings for the shared fields between the old and
   * new schema haven't changed.
   * 
   * @param oldSchema
   * @param newSchema
   * @return true if the mappings are compatible, false if not.
   */
  private static boolean mappingCompatible(EntitySchema oldSchema,
      EntitySchema newSchema) {
    for (FieldMapping oldFieldMapping : oldSchema.getColumnMappingDescriptor()
        .getFieldMappings()) {
      FieldMapping newFieldMapping = newSchema.getColumnMappingDescriptor()
          .getFieldMapping(oldFieldMapping.getFieldName());
      if (newFieldMapping != null) {
        if (!oldFieldMapping.equals(newFieldMapping)) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean fieldsEqual(Field field1, FieldMapping field1Mapping,
      Field field2, FieldMapping field2Mapping) {
    // if names aren't equal, return false
    if (!field1.name().equals(field2.name())) {
      return false;
    }
    // if schemas aren't equal, return false
    if (!AvroUtils.avroSchemaTypesEqual(field1.schema(), field2.schema())) {
      return false;
    }
    // if field mappings aren't equal, return false
    if (!Objects.equal(field1Mapping, field2Mapping)) {
      return false;
    }
    // if one default value is null and the other isn't, return false
    if ((field1.defaultValue() != null && field2.defaultValue() == null)
        || (field1.defaultValue() == null && field2.defaultValue() != null)) {
      return false;
    }
    // if both default values are not null, and the default values are not
    // equal, return false
    if ((field1.defaultValue() != null && field2.defaultValue() != null)
        && !field1.defaultValue().equals(field2.defaultValue())) {
      return false;
    }
    // Fields are equal, return true
    return true;
  }
}
