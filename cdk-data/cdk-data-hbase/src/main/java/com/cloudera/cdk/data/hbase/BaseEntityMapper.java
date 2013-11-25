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
package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.DatasetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;

import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.dao.EntitySchema;
import com.cloudera.cdk.data.dao.EntitySchema.FieldMapping;
import com.cloudera.cdk.data.dao.KeySchema;
import com.cloudera.cdk.data.dao.MappingType;

/**
 * A base implementation of EntityMapper, that uses the provided EntitySerDe and
 * KeyBuilderFactory to map Key/Entity pairs to HBase puts, and HBase results to
 * KeyEntity pairs.
 * 
 * @param <K>
 *          The key type
 * @param <E>
 *          The entity type
 */
public class BaseEntityMapper<E> implements EntityMapper<E> {

  private final KeySchema keySchema;
  private final EntitySchema entitySchema;
  private final KeySerDe keySerDe;
  private final EntitySerDe<E> entitySerDe;

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
    PartitionKey partitionKey = keySerDe.deserialize(result.getRow());
    EntityComposer.Builder<E> builder = getEntityComposer().getBuilder();
    for (FieldMapping fieldMapping : entitySchema.getFieldMappings()) {
      Object fieldValue;
      if (fieldMapping.getMappingType() == MappingType.KEY) {
        fieldValue = partitionKey.get(Integer.parseInt(fieldMapping
            .getMappingValue()));
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
      } else if (fieldMapping.getDefaultValue() != null) {
        builder
            .put(fieldMapping.getFieldName(), fieldMapping.getDefaultValue());
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
  public PutAction mapFromEntity(E entity) {
    List<PutAction> putActionList = new ArrayList<PutAction>();
    List<Object> keyParts = entitySerDe.getEntityComposer()
        .getPartitionKeyParts(entity);
    PartitionKey partitionKey = keySchema.getPartitionStrategy().partitionKey(
        keyParts.toArray());
    byte[] keyBytes = keySerDe.serialize(partitionKey);
    for (FieldMapping fieldMapping : entitySchema.getFieldMappings()) {
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
    FieldMapping fieldMapping = entitySchema.getFieldMapping(fieldName);
    if (fieldMapping == null) {
      throw new DatasetException("Unknown field in the schema: "
          + fieldName);
    }
    if (fieldMapping.getMappingType() != MappingType.COUNTER) {
      throw new DatasetException("Field is not a counter type: "
          + fieldName);
    }

    byte[] keyBytes = keySerDe.serialize(key);
    Increment increment = new Increment(keyBytes);
    increment.addColumn(fieldMapping.getFamily(), fieldMapping.getQualifier(),
        amount);
    return increment;
  }

  @Override
  public long mapFromIncrementResult(Result result, String fieldName) {
    FieldMapping fieldMapping = entitySchema.getFieldMapping(fieldName);
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
    return entitySchema.getRequiredColumns();
  }

  @Override
  public Set<String> getRequiredColumnFamilies() {
    return entitySchema.getRequiredColumnFamilies();
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
