// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.client.HTablePool;

import com.cloudera.cdk.data.hbase.BaseEntityMapper;
import com.cloudera.cdk.data.hbase.EntityMapper;
import com.cloudera.cdk.data.hbase.SchemaNotFoundException;

/**
 * An AvroEntityManager implementation that can create entity mappers which will
 * map GenericRecord instances.
 */
public class GenericAvroEntityManager extends AvroEntityManager {

  private final Map<TableEntityPair, AvroEntitySchema> entitySchemaMap = new HashMap<TableEntityPair, AvroEntitySchema>();

  public GenericAvroEntityManager(HTablePool tablePool) {
    super(tablePool);
  }

  public GenericAvroEntityManager(HTablePool tablePool,
      String managedSchemaTable) {
    super(tablePool, managedSchemaTable);
  }

  /**
   * All entity schemas we will use for writes must be registered with this
   * manager. This method allows one to register an entity's schema.
   * 
   * @param tableName
   *          The table name of the managed entity.
   * @param entityName
   *          The entity name of the managed entity.
   * @param newSchema
   *          The schema of the managed entity to register.
   * @return this instance, to allow method chaining for registering schemas
   */
  public synchronized GenericAvroEntityManager registerEntitySchema(
      String tableName, String entityName, AvroEntitySchema newSchema) {
    TableEntityPair key = new TableEntityPair(tableName, entityName);
    if (entitySchemaMap.containsKey(key)) {
      AvroEntitySchema alreadyRegisteredSchema = entitySchemaMap.get(key);
      if (!alreadyRegisteredSchema.getAvroSchema().equals(
          newSchema.getAvroSchema())) {
        throw new IllegalArgumentException(
            "Schema already stored for this entity: " + tableName + " "
                + entityName + ", and the schemas don't match.");
      }
    } else {
      entitySchemaMap
          .put(new TableEntityPair(tableName, entityName), newSchema);
    }
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized EntityMapper<GenericRecord, GenericRecord> createEntityMapper(
      String tableName, String entityName) {

    // The schema we will use for writes must be registered with this manager.
    // Attempt go get that schema. If it can't be found, throw an exception.
    AvroEntitySchema entitySchema = entitySchemaMap.get(new TableEntityPair(
        tableName, entityName));
    if (entitySchema == null) {
      throw new SchemaNotFoundException(
          "GenericAvroEntityManager requires an entity "
              + "avro schema to be registered with it to create an entity mapper for "
              + "an entity. Could not find one for entity: " + tableName + ", "
              + entityName);
    }

    // verify that this schema exists in the managed schema table.
    // this method will throw a SchemaNotFoundException if it can't
    // find it.
    int version = getEntityVersion(tableName, entityName, entitySchema);

    // found it, now create and return the entity mapper
    return internalCreateEntityMapper(tableName, entityName,
        GenericRecord.class, GenericRecord.class, version);
  }

  /**
   * A pair class for a managed entity. This is used as a key for the entity
   * schema map.
   */
  private static class TableEntityPair {

    private final String tableName;
    private final String entityName;

    public TableEntityPair(String tableName, String entityName) {
      this.tableName = tableName;
      this.entityName = entityName;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TableEntityPair) {
        TableEntityPair other = (TableEntityPair) o;
        return tableName.equals(other.tableName)
            && entityName.equals(other.entityName);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return tableName.hashCode() + (31 * entityName.hashCode());
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected EntityMapper constructWrappedEntityMapper(AvroKeySchema keySchema,
      AvroEntitySchema readSchema, AvroEntitySchema writeSchema,
      Class keyClass, Class entityClass) {
    AvroKeySerDe<GenericRecord> keySerDe = new AvroKeySerDe<GenericRecord>(
        keySchema.getAvroSchema(), false);
    AvroEntityComposer<GenericRecord> entityComposer = new AvroEntityComposer<GenericRecord>(
        readSchema, false);
    AvroEntitySerDe<GenericRecord> entitySerDe = new AvroEntitySerDe<GenericRecord>(
        entityComposer, readSchema, writeSchema, false);
    return new BaseEntityMapper(keySchema, readSchema, keySerDe, entitySerDe);
  }
}
