// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;

import com.cloudera.cdk.data.hbase.EntitySchema.FieldMapping;

/**
 * A base implementation of EntityMapper, that uses the provided
 * EntitySerDe and KeyBuilderFactory to map Key/Entity pairs to
 * HBase puts, and HBase results to KeyEntity pairs.
 *
 * @param <K> The key type
 * @param <E> The entity type
 */
public class BaseEntityMapper<K, E> implements EntityMapper<K, E> {

  private final KeySchema<?> keySchema;
  private final EntitySchema<?> entitySchema;
  private final KeySerDe<K> keySerDe;
  private final EntitySerDe<E> entitySerDe;

  public BaseEntityMapper(KeySchema<?> keySchema, EntitySchema<?> entitySchema,
      KeySerDe<K> keySerDe, EntitySerDe<E> entitySerDe) {
    this.keySchema = keySchema;
    this.entitySchema = entitySchema;
    this.keySerDe = keySerDe;
    this.entitySerDe = entitySerDe;
  }

  @Override
  public KeyEntity<K, E> mapToEntity(Result result) {
    boolean allNull = true;
    EntityComposer.Builder<E> builder = getEntityComposer().getBuilder();
    for (FieldMapping fieldMapping : entitySchema.getFieldMappings()) {
      Object fieldValue = entitySerDe.deserialize(fieldMapping, result);
      if (fieldValue != null) {
        builder.put(fieldMapping.getFieldName(), fieldValue);
        allNull = false;
      } else if (fieldMapping.getDefaultValue() != null) {
        builder.put(fieldMapping.getFieldName(),
            fieldMapping.getDefaultValue());
      }
    }

    /**
     * If all the fields are null, we must assume this is an empty row. There's no
     * way to differentiate between the case where the row exists but this kind of
     * entity wasn't persisted to the row (where the user would expect a return of
     * null), and the case where an entity was put here with all fields set to null.
     * 
     * This can also happen if the entity was put with a schema that shares no fields
     * with the current schema, or at the very least, it share no fields that were not
     * null with the current schema.
     * 
     * TODO: Think about disallowing puts of all null entity fields and schema
     * migrations where two schemas share no fields in common.
     */
    if (allNull) {
      return null;
    }

    K key = keySerDe.deserialize(result.getRow());
    E entity = builder.build();
    return new KeyEntity<K, E>(key, entity);
  }

  @Override
  public PutAction mapFromEntity(K key, E entity) {
    List<PutAction> putActionList = new ArrayList<PutAction>();
    byte[] keyBytes = keySerDe.serialize(key);
    for (FieldMapping fieldMapping : entitySchema.getFieldMappings()) {
      Object fieldValue = getEntityComposer().extractField(entity,
          fieldMapping.getFieldName());
      if (fieldValue != null) {
        PutAction put = entitySerDe.serialize(keyBytes, fieldMapping,
            fieldValue);
        putActionList.add(put);
      }
    }
    return HBaseUtils.mergePutActions(keyBytes, putActionList);
  }
  
  @Override
  public Increment mapToIncrement(K key, String fieldName,
      long amount) {
    FieldMapping fieldMapping = entitySchema.getFieldMapping(fieldName);
    if (fieldMapping == null) {
      throw new HBaseCommonException("Unknown field in the schema: " + fieldName);
    }
    if (!fieldMapping.isIncrementable()) {
      throw new HBaseCommonException("Field is not an incrementable type: " + fieldName);
    }

    byte[] keyBytes = keySerDe.serialize(key);
    Increment increment = new Increment(keyBytes);
    increment.addColumn(fieldMapping.getFamily(), fieldMapping.getQualifier(), amount);
    return increment;
  }
  
  @Override
  public long mapFromIncrementResult(Result result, String fieldName) {
    FieldMapping fieldMapping = entitySchema.getFieldMapping(fieldName);
    if (fieldMapping == null) {
      throw new HBaseCommonException("Unknown field in the schema: " + fieldName);
    }
    if (!fieldMapping.isIncrementable()) {
      throw new HBaseCommonException("Field is not an incrementable type: " + fieldName);
    }
    return (Long)entitySerDe.deserialize(fieldMapping, result);
  }

  @Override
  public Set<String> getRequiredColumns() {
    return entitySchema.getRequiredColumns();
  }

  @Override
  public Set<String> getRequiredColumnFamilies() {
    return entitySchema.getRequiredColumnFamilies();
  }

  @Override
  public KeySchema<?> getKeySchema() {
    return keySchema;
  }

  @Override
  public EntitySchema<?> getEntitySchema() {
    return entitySchema;
  }
  
  @Override
  public KeySerDe<K> getKeySerDe() {
    return keySerDe;
  }

  @Override
  public EntitySerDe<E> getEntitySerDe() {
    return entitySerDe;
  }

  public EntityComposer<E> getEntityComposer() {
    return entitySerDe.getEntityComposer();
  }
}
