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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.FieldMapping.MappingType;
import org.kitesdk.data.ValidationException;

/**
 * This class handles entity serialization and deserialization. It's able to
 * serialize fields of an entity to PutActions.
 * 
 * @param <E>
 *          The type of the entity
 */
public abstract class EntitySerDe<E> {

  private final EntityComposer<E> entityComposer;

  public EntitySerDe(EntityComposer<E> entityComposer) {
    this.entityComposer = entityComposer;
  }

  /**
   * Serialize an entity's field value to a PutAction.
   * 
   * @param keyBytes
   *          The bytes of the serialized key (needed to construct a PutAction).
   * @param fieldMapping
   *          The FieldMapping that specifies this field's mapping type and
   *          field name.
   * @param fieldValue
   *          The value of the field to serialize.
   * @return The PutAction with column's populated with the field's serialized
   *         values.
   */
  public PutAction serialize(byte[] keyBytes, FieldMapping fieldMapping,
      Object fieldValue) {
    Put put = new Put(keyBytes);
    PutAction putAction = new PutAction(put);
    String fieldName = fieldMapping.getFieldName();
    if (fieldMapping.getMappingType() == MappingType.COLUMN
        || fieldMapping.getMappingType() == MappingType.COUNTER) {
      serializeColumn(fieldName, fieldMapping.getFamily(),
          fieldMapping.getQualifier(), fieldValue, put);
    } else if (fieldMapping.getMappingType() == MappingType.KEY_AS_COLUMN) {
      serializeKeyAsColumn(fieldName, fieldMapping.getFamily(),
          fieldMapping.getPrefix(), fieldValue, put);
    } else if (fieldMapping.getMappingType() == MappingType.OCC_VERSION) {
      serializeOCCColumn(fieldValue, putAction);
    } else {
      throw new ValidationException(
          "Invalid field mapping for field with name: "
              + fieldMapping.getFieldName());
    }
    return putAction;
  }

  /**
   * Deserialize an entity field from the HBase Result.
   * 
   * @param fieldMapping
   *          The FieldMapping that specifies this field's mapping type and
   *          field name.
   * @param result
   *          The HBase Result that represents a row in HBase.
   * @return The field Object we deserialized from the Result.
   */
  public Object deserialize(FieldMapping fieldMapping, Result result) {
    String fieldName = fieldMapping.getFieldName();
    MappingType mappingType = fieldMapping.getMappingType();
    if (mappingType == MappingType.COLUMN || mappingType == MappingType.COUNTER) {
      return deserializeColumn(fieldMapping.getFieldName(),
          fieldMapping.getFamily(), fieldMapping.getQualifier(), result);
    } else if (mappingType == MappingType.KEY_AS_COLUMN) {
      return deserializeKeyAsColumn(fieldMapping.getFieldName(),
          fieldMapping.getFamily(), fieldMapping.getPrefix(), result);
    } else if (mappingType == MappingType.OCC_VERSION) {
      return deserializeOCCColumn(result);
    } else {
      throw new ValidationException(
          "Invalid field mapping for field with name: " + fieldName);
    }
  }

  /**
   * Serialize the column mapped entity field value to bytes.
   * 
   * @param fieldName
   *          The name of the entity's field
   * @param fieldValue
   *          The value to serialize
   * @return The serialized bytes
   */
  public abstract byte[] serializeColumnValueToBytes(String fieldName,
      Object fieldValue);

  /**
   * Serialize a value from a keyAsColumn entity field. The value is keyed on
   * the key.
   * 
   * @param fieldName
   *          The name of the entity's keyAsColumn field
   * @param columnKey
   *          The key of the keyAsColumn field
   * @param keyAsColumnFieldValue
   *          The value pointed to by this key.
   * @return The serialized bytes
   */
  public abstract byte[] serializeKeyAsColumnValueToBytes(String fieldName,
      CharSequence columnKey, Object keyAsColumnFieldValue);

  /**
   * Serialize the keyAsColumn key to bytes.
   * 
   * @param fieldName
   *          The name of the entity's keyAsColumn field
   * @param columnKey
   *          The column key to serialize to bytes
   * @return The serialized bytes.
   */
  public abstract byte[] serializeKeyAsColumnKeyToBytes(String fieldName,
      CharSequence columnKey);

  /**
   * Deserialize a column mapped entity field's bytes to its type.
   * 
   * @param fieldName
   *          The name of the entity's field
   * @param columnBytes
   *          The bytes to deserialize
   * @return The field value we've deserialized.
   */
  public abstract Object deserializeColumnValueFromBytes(String fieldName,
      byte[] columnBytes);

  /**
   * Deserialize a value from a keyAsColumn entity field. The value is keyed on
   * key.
   * 
   * @param fieldName
   *          The name of the entity's keyAsColumn field
   * @param columnKeyBytes
   *          The key bytes of the keyAsColumn field
   * @param columnValueBytes
   *          The value bytes to deserialize
   * @return The keyAsColumn value pointed to by key.
   */
  public abstract Object deserializeKeyAsColumnValueFromBytes(String fieldName,
      byte[] columnKeyBytes, byte[] columnValueBytes);

  /**
   * Deserialize the keyAsColumn key from the qualifier.
   * 
   * @param fieldName
   *          The name of the keyAsColumn field
   * @param columnKeyBytes
   *          The bytes of the qualifier
   * @return The deserialized CharSequence
   */
  public abstract CharSequence deserializeKeyAsColumnKeyFromBytes(
      String fieldName, byte[] columnKeyBytes);

  /**
   * If a field has a default value, returns the default value. Otherwise,
   * returns null.
   *
   * @param fieldName
   *          The name of the field to return the default value for.
   * @return The default value.
   */
  public abstract Object getDefaultValue(String fieldName);

  /**
   * Get the EntityComposer this EntitySerDe uses to compose entity fields.
   * 
   * @return The EntityComposer
   */
  public EntityComposer<E> getEntityComposer() {
    return entityComposer;
  }

  /**
   * Serialize the column value, and update the Put with the serialized bytes.
   * 
   * @param fieldName
   *          The name of the entity field we are serializing
   * @param family
   *          The column family this field maps to
   * @param qualifier
   *          The qualifier this field maps to
   * @param fieldValue
   *          The value we are serializing
   * @param put
   *          The Put we are updating with the serialized bytes.
   */
  private void serializeColumn(String fieldName, byte[] family,
      byte[] qualifier, Object fieldValue, Put put) {
    // column mapping, so simply serialize the value and add the bytes
    // to the put.
    byte[] bytes = serializeColumnValueToBytes(fieldName, fieldValue);
    put.add(family, qualifier, bytes);
  }

  /**
   * Serialize a keyAsColumn field, and update the put with the serialized bytes
   * from each subfield of the keyAsColumn value.
   * 
   * @param fieldName
   *          The name of the entity field we are serializing
   * @param family
   *          The column family this field maps to
   * @param prefix
   *          An optional prefix each column qualifier should be prefixed with
   * @param fieldValue
   *          The value we are serializing
   * @param put
   *          The put to update with the serialized bytes.
   */
  private void serializeKeyAsColumn(String fieldName, byte[] family,
      String prefix, Object fieldValue, Put put) {
    // keyAsColumn mapping, so extract each value from the keyAsColumn field
    // using the entityComposer, serialize them, and them to the put.
    Map<CharSequence, Object> keyAsColumnValues = entityComposer
        .extractKeyAsColumnValues(fieldName, fieldValue);
    for (Entry<CharSequence, Object> entry : keyAsColumnValues.entrySet()) {
      CharSequence qualifier = entry.getKey();
      byte[] qualifierBytes;
      byte[] columnKeyBytes = serializeKeyAsColumnKeyToBytes(fieldName,
          qualifier);
      if (prefix != null) {
        byte[] prefixBytes = prefix.getBytes();
        qualifierBytes = new byte[prefixBytes.length + columnKeyBytes.length];
        System.arraycopy(prefixBytes, 0, qualifierBytes, 0, prefixBytes.length);
        System.arraycopy(columnKeyBytes, 0, qualifierBytes, prefixBytes.length,
            columnKeyBytes.length);
      } else {
        qualifierBytes = columnKeyBytes;
      }

      // serialize the value, and add it to the put.
      byte[] bytes = serializeKeyAsColumnValueToBytes(fieldName, qualifier,
          entry.getValue());
      put.add(family, qualifierBytes, bytes);
    }
  }

  /**
   * Serialize the OCC column value, and update the putAction with the
   * serialized bytes.
   * 
   * @param fieldValue
   *          The value to serialize
   * @param putAction
   *          The PutAction to update.
   */
  private void serializeOCCColumn(Object fieldValue, PutAction putAction) {
    // OCC Version mapping, so serialize as a long to the version check
    // column qualifier in the system column family.
    Long currVersion = (Long) fieldValue;
    VersionCheckAction versionCheckAction = new VersionCheckAction(currVersion);
    putAction.getPut().add(Constants.SYS_COL_FAMILY,
        Constants.VERSION_CHECK_COL_QUALIFIER, Bytes.toBytes(currVersion + 1));
    putAction.setVersionCheckAction(versionCheckAction);
  }

  /**
   * Deserialize the entity field that has a column mapping.
   * 
   * @param fieldName
   *          The name of the entity's field we are deserializing.
   * @param family
   *          The column family this field is mapped to
   * @param qualifier
   *          The column qualifier this field is mapped to
   * @param result
   *          The HBase Result that represents a row in HBase.
   * @return The deserialized field value
   */
  private Object deserializeColumn(String fieldName, byte[] family,
      byte[] qualifier, Result result) {
    byte[] bytes = result.getValue(family, qualifier);
    if (bytes == null) {
      return getDefaultValue(fieldName);
    } else {
      return deserializeColumnValueFromBytes(fieldName, bytes);
    }
  }

  /**
   * Deserialize the entity field that has a keyAsColumn mapping.
   * 
   * @param fieldName
   *          The name of the entity's field we are deserializing.
   * @param family
   *          The column family this field is mapped to
   * @param prefix
   *          The column qualifier prefix each
   * @param result
   *          The HBase Result that represents a row in HBase.
   * @return The deserialized entity field value.
   */
  private Object deserializeKeyAsColumn(String fieldName, byte[] family,
      String prefix, Result result) {
    // Construct a map of keyAsColumn field values. From this we'll be able
    // to use the entityComposer to construct the entity field value.
    byte[] prefixBytes = prefix != null ? prefix.getBytes() : null;
    Map<CharSequence, Object> fieldValueAsMap = new HashMap<CharSequence, Object>();
    Map<byte[], byte[]> familyMap = result.getFamilyMap(family);
    for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
      byte[] qualifier = entry.getKey();
      // if the qualifier of this column has a prefix that matches the
      // field prefix, then remove the prefix from the qualifier.
      if (prefixBytes != null
          && qualifier.length > prefixBytes.length
          && Arrays.equals(Arrays.copyOf(qualifier, prefixBytes.length),
              prefixBytes)) {
        qualifier = Arrays.copyOfRange(qualifier, prefixBytes.length,
            qualifier.length);
      }
      byte[] columnBytes = entry.getValue();
      CharSequence keyAsColumnKey = deserializeKeyAsColumnKeyFromBytes(
          fieldName, qualifier);
      Object keyAsColumnValue = deserializeKeyAsColumnValueFromBytes(fieldName,
          qualifier, columnBytes);
      fieldValueAsMap.put(keyAsColumnKey, keyAsColumnValue);
    }
    // Now build the entity field from the fieldValueAsMap.
    return entityComposer.buildKeyAsColumnField(fieldName, fieldValueAsMap);
  }

  /**
   * Deserialize the OCC column value from the Result.
   * 
   * @param result
   *          The HBase Result that represents a row in HBase.
   * @return The deserialized OCC field value
   */
  private Object deserializeOCCColumn(Result result) {
    byte[] versionBytes = result.getValue(Constants.SYS_COL_FAMILY,
        Constants.VERSION_CHECK_COL_QUALIFIER);
    if (versionBytes == null) {
      return null;
    } else {
      return Bytes.toLong(versionBytes);
    }
  }
}
