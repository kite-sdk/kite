// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

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
 * 
 * @param <RAW_SCHEMA>
 *          The type of the RAW_SCHEMA that is parsed by the SchemaParser to
 *          generate EntitySchema instances.
 */
public class EntitySchema<RAW_SCHEMA> {

  private final boolean isTransactional;
  private final Collection<String> tables;
  private final Map<String, FieldMapping> fieldMappings = new HashMap<String, FieldMapping>();
  private final RAW_SCHEMA rawSchema;

  /**
   * Constructs the EntitySchema
   * 
   * @param tables
   *          The tables this EntitySchema can be persisted to
   * @param rawSchema
   *          The raw schema type that underlies the EntitySchema implementation
   * @param fieldMappings
   *          The list of FieldMappings that specify how each field maps to an
   *          HBase row
   * @param isTransactional
   *          Specifies whether this entity participates in transactions
   */
  public EntitySchema(Collection<String> tables, RAW_SCHEMA rawSchema,
      Collection<FieldMapping> fieldMappings, boolean isTransactional) {
    this.tables = tables;
    this.rawSchema = rawSchema;
    this.isTransactional = isTransactional;
    for (FieldMapping fieldMapping : fieldMappings) {
      this.fieldMappings.put(fieldMapping.getFieldName(), fieldMapping);
    }
  }

  /**
   * Specifies whether this EntitySchema will participate in transactions.
   * 
   * @return True if it should participate in transactions. Otherwise false.
   */
  public boolean isTransactional() {
    return isTransactional;
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
  public RAW_SCHEMA getRawSchema() {
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
      if (MappingType.COLUMN == fieldMapping.getMappingType()) {
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
   * A field mapping represents a type that specifies how a schema field maps to
   * a column in HBase.
   */
  public static class FieldMapping {

    private final String fieldName;
    private final MappingType mappingType;
    private final String mappingValue;
    private final boolean incrementable;
    private final Object defaultValue;
    private final String prefix;
    private final byte[] family;
    private final byte[] qualifier;

    public FieldMapping(String fieldName, MappingType mappingType,
        String mappingValue, Object defaultValue, String prefix,
        boolean incrementable) {
      this.fieldName = fieldName;
      this.mappingType = mappingType;
      this.mappingValue = mappingValue;
      this.defaultValue = defaultValue;
      this.incrementable = incrementable;
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

    public boolean isIncrementable() {
      return incrementable;
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
      if (mappingType == MappingType.OCC_VERSION) {
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
      if (mappingType == MappingType.OCC_VERSION) {
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
