// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTablePool;

import com.cloudera.cdk.data.hbase.BaseEntityMapper;
import com.cloudera.cdk.data.hbase.EntityMapper;
import com.cloudera.cdk.data.hbase.HBaseCommonException;
import com.cloudera.cdk.data.hbase.KeySerDe;
import com.cloudera.cdk.data.hbase.transactions.TransactionManager;

/**
 * An AvroEntityManager implementation that can create entity mappers which will
 * map SpecificRecord instances.
 */
public class SpecificAvroEntityManager extends AvroEntityManager {

  private static Log LOG = LogFactory.getLog(AvroEntityManager.class);

  private static final AvroKeyEntitySchemaParser schemaParser = new AvroKeyEntitySchemaParser();

  public SpecificAvroEntityManager(TransactionManager transactionManager,
      HTablePool tablePool) {
    super(transactionManager, tablePool);
  }

  public SpecificAvroEntityManager(TransactionManager transactionManager,
      HTablePool tablePool, String managedSchemaTable) {
    super(transactionManager, tablePool, managedSchemaTable);
  }

  @Override
  @SuppressWarnings({ "unchecked" })
  public <K, E> EntityMapper<K, E> createEntityMapper(String tableName,
      String entityName) {

    Schema keySchema = getKeySchema(tableName, entityName).getAvroSchema();
    Schema entitySchema = getEntitySchema(tableName, entityName)
        .getAvroSchema();
    
    String keyClassName = keySchema.getFullName();
    String entityClassName = entitySchema.getFullName();
    try {
      Class<K> keyClass = (Class<K>) Class.forName(keyClassName);
      Class<E> entityClass = (Class<E>) Class.forName(entityClassName);
      String entitySchemaString = entityClass.getField("SCHEMA$").get(null).toString();
      AvroEntitySchema actualSchema = schemaParser.parseEntity(entitySchemaString);

      int version = getEntityVersion(tableName, entityName, actualSchema);
      return internalCreateEntityMapper(tableName, entityName,
          keyClass, entityClass, version);
    } catch (ClassNotFoundException e) {
      String msg = "Key or entity class not found. Make sure the specific "
          + "record instances are on the classpath.";
      LOG.error(msg, e);
      throw new HBaseCommonException(msg, e);
    } catch (SecurityException e) {
      String msg = "Cannot access key or entity class.";
      LOG.error(msg, e);
      throw new HBaseCommonException(msg, e);
    } catch (NoSuchFieldException e) {
      String msg = "SCHEMA$ field not found in the entity class: "
          + entityClassName;
      LOG.error(msg, e);
      throw new HBaseCommonException(msg, e);
    } catch (IllegalAccessException e) {
      String msg = "Not allowed to access SCHEMA$ field in the entity class: "
          + entityClassName;
      LOG.error(msg, e);
      throw new HBaseCommonException(msg, e);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected EntityMapper constructWrappedEntityMapper(AvroKeySchema keySchema,
      AvroEntitySchema readSchema, AvroEntitySchema writeSchema,
      Class keyClass, Class entityClass) {
    
    keySchema = AvroUtils.mergeSpecificStringTypes(keyClass, keySchema);
    readSchema = AvroUtils.mergeSpecificStringTypes(entityClass, readSchema);
    
    KeySerDe keySerDe = new AvroKeySerDe(keySchema.getAvroSchema(), true);
    AvroEntityComposer entityComposer = new AvroEntityComposer(readSchema, true);
    AvroEntitySerDe entitySerDe = new AvroEntitySerDe(entityComposer,
        readSchema, writeSchema, true);
    return new BaseEntityMapper(keySchema, readSchema, keySerDe, entitySerDe);
  }
}
