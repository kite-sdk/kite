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

import com.google.common.base.Objects;
import java.io.IOException;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.cloudera.cdk.data.dao.EntitySchema;
import com.cloudera.cdk.data.dao.HBaseCommonException;
import com.cloudera.cdk.data.dao.SchemaValidationException;

/**
 * An EntitySchema implementation powered by Avro.
 */
public class AvroEntitySchema extends EntitySchema {

  private final Schema schema;

  /**
   * Constructor for the AvroEntitySchema.
   * 
   * @param tables
   *          The tables this EntitySchema can be persisted to
   * @param schema
   *          The Avro Schema that underlies this EntitySchema implementation
   * @param rawSchema
   *          The Avro Schema as a string that underlies the EntitySchema
   *          implementation
   * @param fieldMappings
   *          The list of FieldMappings that specify how each field maps to an
   *          HBase row
   * @param isTransactional
   *          Specifies whether this entity participates in transactions
   */
  public AvroEntitySchema(Collection<String> tables, Schema schema,
      String rawSchema, Collection<FieldMapping> fieldMappings,
      boolean isTransactional) {
    super(tables, rawSchema, fieldMappings, isTransactional);
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
  
  public Schema getKeyAvroSchema() {
    return null;
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
        if (!fieldsEqual(field, getFieldMapping(field.name()),
            entitySchemaField, other.getFieldMapping(entitySchemaField.name()))) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean compatible(EntitySchema entitySchema) {
    if (!mappingCompatible(getRawSchema(), entitySchema.getRawSchema())) {
      return false;
    }
    AvroEntitySchema avroEntitySchema = (AvroEntitySchema) entitySchema;
    if (!avroReadWriteSchemasCompatible(schema,
        avroEntitySchema.getAvroSchema())) {
      return false;
    }
    if (!avroReadWriteSchemasCompatible(avroEntitySchema.getAvroSchema(),
        schema)) {
      return false;
    }
    return true;
  }

  /**
   * Ensure that the field mappings haven't changed between the oldSchemaString
   * and the newSchemaString.
   * 
   * @param oldSchemaString
   * @param newSchemaString
   * @return true if the mappings are compatible, false if not.
   */
  private static boolean mappingCompatible(String oldSchemaString,
      String newSchemaString) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode oldSchema;
    JsonNode newSchema;
    try {
      oldSchema = mapper.readValue(oldSchemaString, JsonNode.class);
      newSchema = mapper.readValue(newSchemaString, JsonNode.class);
    } catch (IOException e) {
      throw new SchemaValidationException(
          "Schemas not proper JSON in mappingCompatible", e);
    }

    JsonNode oldSchemaFields = oldSchema.get("fields");
    JsonNode newSchemaFields = newSchema.get("fields");
    for (JsonNode oldSchemaField : oldSchemaFields) {
      String oldSchemaFieldName = oldSchemaField.get("name").getTextValue();
      for (JsonNode newSchemaField : newSchemaFields) {
        if (oldSchemaFieldName
            .equals(newSchemaField.get("name").getTextValue())) {
          if (!oldSchemaField.get("mapping").equals(
              newSchemaField.get("mapping"))) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Returns true if the writer and reader schema are compatible with each
   * other, following the avro specification.
   * 
   * @param writer
   *          writer schema
   * @param reader
   *          reader schema
   * @return True if compatible, false if not.
   */
  private static boolean avroReadWriteSchemasCompatible(Schema writer,
      Schema reader) {
    Symbol rootSymbol;
    try {
      ResolvingGrammarGenerator g = new ResolvingGrammarGenerator();
      rootSymbol = g.generate(writer, reader);
    } catch (IOException e) {
      throw new HBaseCommonException("IOException while generating grammar.", e);
    }

    return !hasErrorSymbol(rootSymbol);
  }

  /**
   * Determine if the symbol tree has an error symbol in it. This would indicate
   * that the two schemas are not compatible.
   * 
   * @param rootSymbol
   *          The root symbol to traverse from to look for an error symbol.
   * @return true if an error symbol exists in the tree.
   */
  private static boolean hasErrorSymbol(Symbol rootSymbol) {
    if (rootSymbol.production == null) {
      return false;
    }
    for (Symbol s : rootSymbol.production) {
      if (s == rootSymbol) {
        continue;
      }
      if (s.getClass().equals(Symbol.ErrorAction.class)) {
        return true;
      } else {
        if (s.production != null) {
          for (Symbol subSymbol : s.production) {
            if (hasErrorSymbol(subSymbol)) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  private static boolean fieldsEqual(Field field1, FieldMapping field1Mapping,
      Field field2, FieldMapping field2Mapping) {
    // if names aren't equal, return false
    if (!field1.name().equals(field2.name())) {
      return false;
    }
    // if schemas aren't equal, return false
    if (!avroSchemasEqual(field1.schema(), field2.schema())) {
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

  private static boolean avroSchemasEqual(Schema schema1, Schema schema2) {
    if (schema1.getType() != schema2.getType()) {
      // if the types aren't equal, no need to go further. Return false
      return false;
    }

    if (schema1.getType() == Schema.Type.ENUM
        || schema1.getType() == Schema.Type.FIXED) {
      // Enum and Fixed types schemas should be equal using the Schema.equals
      // method.
      return schema1.equals(schema2);
    }
    if (schema1.getType() == Schema.Type.ARRAY) {
      // Avro element schemas should be equal, which is tested by recursively
      // calling this method.
      return avroSchemasEqual(schema1.getElementType(),
          schema2.getElementType());
    } else if (schema1.getType() == Schema.Type.MAP) {
      // Map type values schemas should be equal, which is tested by recursively
      // calling this method.
      return avroSchemasEqual(schema1.getValueType(), schema2.getValueType());
    } else if (schema1.getType() == Schema.Type.UNION) {
      // Compare Union fields in the same position by comparing their schemas
      // recursively calling this method.
      if (schema1.getTypes().size() != schema2.getTypes().size()) {
        return false;
      }
      for (int i = 0; i < schema1.getTypes().size(); i++) {
        if (!avroSchemasEqual(schema1.getTypes().get(i), schema2.getTypes()
            .get(i))) {
          return false;
        }
      }
      return true;
    } else if (schema1.getType() == Schema.Type.RECORD) {
      // Compare record fields that match in name by comparing their schemas
      // recursively calling this method.
      for (Field field1 : schema1.getFields()) {
        Field field2 = schema2.getField(field1.name());
        if (field2 == null) {
          return false;
        }
        if (!avroSchemasEqual(field1.schema(), field2.schema())) {
          return false;
        }
      }
      return true;
    } else {
      // All other types are primitive, so them matching in type is enough.
      return true;
    }
  }
}
