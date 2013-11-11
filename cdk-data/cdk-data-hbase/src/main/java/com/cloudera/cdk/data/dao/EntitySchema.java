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
package com.cloudera.cdk.data.dao;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An EntitySchema is the parsed schema that contains the properties of an HBase
 * Common entity schema.
 */
public class EntitySchema {

  private final Collection<String> tables;
  private final Map<String, FieldMapping> fieldMappings = new HashMap<String, FieldMapping>();
  private final String rawSchema;
  private final String name;

  /**
   * Constructs the EntitySchema
   * 
   * @param tables
   *          The tables this EntitySchema can be persisted to
   * @param name
   *          The name of the entity schema
   * @param rawSchema
   *          The raw schema type that underlies the EntitySchema implementation
   * @param fieldMappings
   *          The list of FieldMappings that specify how each field maps to an
   *          HBase row
   */
  public EntitySchema(Collection<String> tables, String name, String rawSchema,
      Collection<FieldMapping> fieldMappings) {
    this.tables = tables;
    this.name = name;
    this.rawSchema = rawSchema;
    validateFieldMappings(fieldMappings);
    for (FieldMapping fieldMapping : fieldMappings) {
      this.fieldMappings.put(fieldMapping.getFieldName(), fieldMapping);
    }
  }

  /**
   * Get the tables this EntitySchema can be persisted to.
   * 
   * @return The list of tables.
   */
  public Collection<String> getTables() {
    return tables;
  }

  /**
   * Get the name of this EntitySchema
   * 
   * @return The name
   */
  public String getName() {
    return name;
  }

  /**
   * Get the FieldMapping for the specified fieldName. Returns null if one
   * doesn't exist.
   * 
   * @param fieldName
   *          The field name to get the FieldMapping for
   * @return The FieldMapping, or null if one doesn't exist fo rthe fieldName.
   */
  public FieldMapping getFieldMapping(String fieldName) {
    return fieldMappings.get(fieldName);
  }

  /**
   * Get the FieldMappings for this schema.
   * 
   * @return The collection of FieldMappings
   */
  public Collection<FieldMapping> getFieldMappings() {
    return fieldMappings.values();
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
   * Get the HBase columns required by this schema.
   * 
   * @return The set of columns
   */
  public Set<String> getRequiredColumns() {
    Set<String> set = new HashSet<String>();
    for (FieldMapping fieldMapping : fieldMappings.values()) {
      if (MappingType.COLUMN == fieldMapping.getMappingType()
          || MappingType.COUNTER == fieldMapping.getMappingType()) {
        set.add(fieldMapping.getMappingValue());
      } else if (MappingType.KEY_AS_COLUMN == fieldMapping.getMappingType()) {
        String family = fieldMapping.getMappingValue().split(":", 1)[0];
        family = family + ":";
        set.add(family);
      } else if (MappingType.OCC_VERSION == fieldMapping.getMappingType()) {
        set.add(new String(Constants.SYS_COL_FAMILY) + ":"
            + new String(Constants.VERSION_CHECK_COL_QUALIFIER));
      }
    }
    return set;
  }

  /**
   * Get the HBase column families required by this schema.
   * 
   * @return The set of column families.
   */
  public Set<String> getRequiredColumnFamilies() {
    Set<String> set = new HashSet<String>();
    Set<String> columnSet = getRequiredColumns();
    for (String column : columnSet) {
      set.add(column.split(":")[0]);
    }
    return set;
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

  /**
   * Validate that the field mappings provided for this schema are compatible
   * with a valid schema. The rules are:
   * 
   * <pre>
   * 1. An entity schema can't contain multiple occVersion mapping fields
   * 2. An entity schema can't contain both an occVersion field and a counter 
   *    field.
   * </pre>
   * 
   * This method will throw a SchemaValidationException if any of these
   * rules are violated. Otherwise, no exception is thrown.
   * 
   * @param fieldMappings The collection of FieldMappings to validate
   */
  private void validateFieldMappings(Collection<FieldMapping> fieldMappings) {
    boolean hasOCCVersion = false;
    boolean hasCounter = false;

    for (FieldMapping fieldMapping : fieldMappings) {
      if (fieldMapping.getMappingType() == MappingType.OCC_VERSION) {
        if (hasOCCVersion) {
          throw new SchemaValidationException(
              "Schema can't contain multiple occVersion fields.");
        }
        if (hasCounter) {
          throw new SchemaValidationException(
              "Schema can't contain both an occVersion field and a counter field.");
        }
        hasOCCVersion = true;
      } else if (fieldMapping.getMappingType() == MappingType.COUNTER) {
        if (hasOCCVersion) {
          throw new SchemaValidationException(
              "Schema can't contain both an occVersion field and a counter field.");
        }
        hasCounter = true;
      }
    }
  }

  /**
   * A field mapping represents a type that specifies how a schema field maps to
   * a column in HBase.
   */
  public static class FieldMapping {

    private final String fieldName;
    private final MappingType mappingType;
    private final String mappingValue;
    private final Object defaultValue;
    private final String prefix;
    private final byte[] family;
    private final byte[] qualifier;

    public FieldMapping(String fieldName, MappingType mappingType,
        String mappingValue, Object defaultValue, String prefix) {
      this.fieldName = fieldName;
      this.mappingType = mappingType;
      this.mappingValue = mappingValue;
      this.defaultValue = defaultValue;
      this.prefix = prefix;
      this.family = getFamilyFromMappingValue(mappingValue);
      this.qualifier = getQualifierFromMappingValue(mappingValue);
    }

    public String getFieldName() {
      return fieldName;
    }

    public MappingType getMappingType() {
      return mappingType;
    }

    public String getMappingValue() {
      return mappingValue;
    }

    public Object getDefaultValue() {
      return defaultValue;
    }

    public String getPrefix() {
      return prefix;
    }

    public byte[] getFamily() {
      return family;
    }

    public byte[] getQualifier() {
      return qualifier;
    }

    private byte[] getFamilyFromMappingValue(String mappingValue) {
      if (mappingType == MappingType.KEY) {
        return null;
      } else if (mappingType == MappingType.OCC_VERSION) {
        return Constants.SYS_COL_FAMILY;
      } else {
        String[] familyQualifier = mappingValue.split(":", 2);
        byte[] family;
        try {
          family = familyQualifier[0].getBytes("UTF-8");
        } catch (UnsupportedEncodingException exc) {
          throw new SchemaValidationException(
              "fieldType Must support UTF-8 encoding", exc);
        }
        return family;
      }
    }

    private byte[] getQualifierFromMappingValue(String mappingValue) {
      if (mappingType == MappingType.KEY) {
        return null;
      } else if (mappingType == MappingType.OCC_VERSION) {
        return Constants.VERSION_CHECK_COL_QUALIFIER;
      } else {
        String[] familyQualifier = mappingValue.split(":", 2);
        byte[] qualifier;
        try {
          qualifier = familyQualifier.length == 1 ? new byte[0]
              : familyQualifier[1].getBytes("UTF-8");
        } catch (UnsupportedEncodingException exc) {
          throw new SchemaValidationException(
              "fieldType Must support UTF-8 encoding", exc);
        }
        return qualifier;
      }
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((defaultValue == null) ? 0 : defaultValue.hashCode());
      result = prime * result + Arrays.hashCode(family);
      result = prime * result
          + ((fieldName == null) ? 0 : fieldName.hashCode());
      result = prime * result
          + ((mappingType == null) ? 0 : mappingType.hashCode());
      result = prime * result
          + ((mappingValue == null) ? 0 : mappingValue.hashCode());
      result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
      result = prime * result + Arrays.hashCode(qualifier);
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
      FieldMapping other = (FieldMapping) obj;
      if (defaultValue == null) {
        if (other.defaultValue != null)
          return false;
      } else if (!defaultValue.equals(other.defaultValue))
        return false;
      if (!Arrays.equals(family, other.family))
        return false;
      if (fieldName == null) {
        if (other.fieldName != null)
          return false;
      } else if (!fieldName.equals(other.fieldName))
        return false;
      if (mappingType != other.mappingType)
        return false;
      if (mappingValue == null) {
        if (other.mappingValue != null)
          return false;
      } else if (!mappingValue.equals(other.mappingValue))
        return false;
      if (prefix == null) {
        if (other.prefix != null)
          return false;
      } else if (!prefix.equals(other.prefix))
        return false;
      if (!Arrays.equals(qualifier, other.qualifier))
        return false;
      return true;
    }
  }
}
