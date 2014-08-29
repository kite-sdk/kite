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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.FieldMapping.MappingType;
import org.kitesdk.data.spi.PartitionKey;

/**
 * A base implementation of EntityMapper, that uses the provided EntitySerDe and
 * KeySerDe to map entities to HBase puts, and HBase results to Entities
 * 
 * @param <E>
 *          The entity type
 */
public class BaseEntityMapper<E> implements EntityMapper<E> {

  private final KeySchema keySchema;
  private final EntitySchema entitySchema;
  private final KeySerDe keySerDe;
  private final EntitySerDe<E> entitySerDe;

  public BaseEntityMapper(EntitySchema entitySchema, EntitySerDe<E> entitySerDe) {
    this(null, entitySchema, null, entitySerDe);
  }

  public BaseEntityMapper(KeySchema keySchema, EntitySchema entitySchema,
      KeySerDe keySerDe, EntitySerDe<E> entitySerDe) {
    this.keySchema = keySchema;
    this.entitySchema = entitySchema;
    this.keySerDe = keySerDe;
    this.entitySerDe = entitySerDe;
  }

  @Override
  public E mapToEntity(Result result) {
    boolean allNull = true;
    PartitionKey partitionKey;
    if (keySerDe == null) {
      partitionKey = null;
    } else {
      partitionKey = keySerDe.deserialize(result.getRow());
    }
    EntityComposer.Builder<E> builder = getEntityComposer().getBuilder();
    for (FieldMapping fieldMapping : entitySchema.getColumnMappingDescriptor()
        .getFieldMappings()) {
      Object fieldValue;
      if (fieldMapping.getMappingType() == MappingType.KEY) {
        if (partitionKey != null) {
          // KEY field mappings are always associated with identity mappers,
          // which is enforced by the DatasetDescriptor. Get the value from the
          // key at the correct position.
          fieldValue = partitionKey.get(
              keySchema.position(fieldMapping.getFieldName()));
        } else {
          // This should never happen. The partitionKey is null only when
          // a keySerDe hasn't been set, which indicates we have an entity
          // without a key. That means there should be no KEY mapping types, but
          // we somehow got here.
          throw new DatasetException("[BUG] Key-mapped field and null key");
        }
      } else {
        fieldValue = entitySerDe.deserialize(fieldMapping, result);
      }
      if (fieldValue != null) {
        builder.put(fieldMapping.getFieldName(), fieldValue);
        // reading a key doesn't count for a row not being null. for example,
        // composite records, where one composite is null will have its keys
        // filled in only. This should be a null composite component.
        if (fieldMapping.getMappingType() != MappingType.KEY) {
          allNull = false;
        }
      }
    }

    /**
     * If all the fields are null, we must assume this is an empty row. There's
     * no way to differentiate between the case where the row exists but this
     * kind of entity wasn't persisted to the row (where the user would expect a
     * return of null), and the case where an entity was put here with all
     * fields set to null.
     * 
     * This can also happen if the entity was put with a schema that shares no
     * fields with the current schema, or at the very least, it share no fields
     * that were not null with the current schema.
     * 
     * TODO: Think about disallowing puts of all null entity fields and schema
     * migrations where two schemas share no fields in common.
     */
    if (allNull) {
      return null;
    }

    E entity = builder.build();
    return entity;
  }

  @Override
  public PartitionKey mapToKey(E entity) {
    return getEntityComposer()
        .extractKey(keySchema.getPartitionStrategy(), entity);
  }

  @Override
  public PutAction mapFromEntity(E entity) {
    List<PutAction> putActionList = new ArrayList<PutAction>();
    byte[] keyBytes;
    if (keySchema == null || keySerDe == null) {
      keyBytes = new byte[] { (byte) 0 };
    } else {
      keyBytes = keySerDe.serialize(mapToKey(entity));
    }
    for (FieldMapping fieldMapping : entitySchema.getColumnMappingDescriptor()
        .getFieldMappings()) {
      if (fieldMapping.getMappingType() == MappingType.KEY) {
        continue;
      }
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
  public Increment mapToIncrement(PartitionKey key, String fieldName,
      long amount) {
    FieldMapping fieldMapping = entitySchema.getColumnMappingDescriptor()
        .getFieldMapping(fieldName);
    if (fieldMapping == null) {
      throw new DatasetException("Unknown field in the schema: "
          + fieldName);
    }
    if (fieldMapping.getMappingType() != MappingType.COUNTER) {
      throw new DatasetException("Field is not a counter type: "
          + fieldName);
    }

    byte[] keyBytes;
    if (keySerDe == null) {
      keyBytes = new byte[] { (byte) 0 };
    } else {
      keyBytes = keySerDe.serialize(key);
    }
    Increment increment = new Increment(keyBytes);
    increment.addColumn(fieldMapping.getFamily(), fieldMapping.getQualifier(),
        amount);
    return increment;
  }

  @Override
  public long mapFromIncrementResult(Result result, String fieldName) {
    FieldMapping fieldMapping = entitySchema.getColumnMappingDescriptor()
        .getFieldMapping(fieldName);
    if (fieldMapping == null) {
      throw new DatasetException("Unknown field in the schema: "
          + fieldName);
    }
    if (fieldMapping.getMappingType() != MappingType.COUNTER) {
      throw new DatasetException("Field is not a counter type: "
          + fieldName);
    }
    return (Long) entitySerDe.deserialize(fieldMapping, result);
  }

  @Override
  public Set<String> getRequiredColumns() {
    return entitySchema.getColumnMappingDescriptor().getRequiredColumns();
  }

  @Override
  public Set<String> getRequiredColumnFamilies() {
    return entitySchema.getColumnMappingDescriptor()
        .getRequiredColumnFamilies();
  }

  @Override
  public KeySchema getKeySchema() {
    return keySchema;
  }

  @Override
  public EntitySchema getEntitySchema() {
    return entitySchema;
  }

  @Override
  public KeySerDe getKeySerDe() {
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
