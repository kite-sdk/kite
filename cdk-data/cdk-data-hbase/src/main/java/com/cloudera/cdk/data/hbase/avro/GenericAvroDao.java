// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

import java.io.InputStream;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.client.HTablePool;

import com.cloudera.cdk.data.hbase.BaseDao;
import com.cloudera.cdk.data.hbase.BaseEntityMapper;
import com.cloudera.cdk.data.hbase.transactions.TransactionManager;

/**
 * A Dao for Avro's GenericRecords. In this Dao implementation, both the
 * underlying key record type, and the entity type are GenericRecords. This Dao
 * allows us to persist and fetch these GenericRecords to and from HBase.
 */
public class GenericAvroDao extends BaseDao<GenericRecord, GenericRecord> {

  private static final AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();

  /**
   * Construct a GenericAvroDao.
   * 
   * @param transactionManager
   *          The TransactionManager that will manage transactional entities.
   * @param tablePool
   *          An HTablePool instance to use for connecting to HBase.
   * @param tableName
   *          The name of the table this Dao will read from and write to in
   *          HBase.
   * @param keySchemaStr
   *          The Avro schema that represents the Key structure for row keys in
   *          this table.
   * @param entitySchemaString
   *          The json string representing the special avro record schema, that
   *          contains metadata in annotations of the Avro record fields. See
   *          {@link AvroEntityMapper} for details.
   */
  public GenericAvroDao(TransactionManager transactionManager,
      HTablePool tablePool, String tableName, String keySchemaStr,
      String entitySchemaString) {
    super(transactionManager, tablePool, tableName, buildEntityMapper(
        entitySchemaString, keySchemaStr));
  }

  /**
   * Construct a GenericAvroDao.
   * 
   * @param transactionManager
   *          The TransactionManager that will manage transactional entities.
   * @param tablePool
   *          An HTablePool instance to use for connecting to HBase.
   * @param tableName
   *          The name of the table this Dao will read from and write to in
   *          HBase.
   * @param keySchemaStr
   *          The Avro schema that represents the Key structure for row keys in
   *          this table.
   * @param entitySchemaStream
   *          The InputStream that contains a json string representing the
   *          special avro record schema, that contains metadata in annotations
   *          of the Avro record fields. See {@link AvroEntityMapper} for
   *          details.
   */
  public GenericAvroDao(TransactionManager transactionManager,
      HTablePool tablePool, String tableName, String keySchemaStr,
      InputStream entitySchemaStream) {

    super(transactionManager, tablePool, tableName, buildEntityMapper(
        AvroUtils.inputStreamToString(entitySchemaStream), keySchemaStr));
  }

  /**
   * Construct the GenericAvroDao with an EntityManager, which will provide the
   * entity mapper to this Dao that knows how to map the different entity schema
   * versions defined by the managed schema. The entitySchemaString parameter
   * represents the schema to use for writes.
   * 
   * @param transactionManager
   *          The TransactionManager that will manage transactional entities.
   * @param tablePool
   *          An HTabePool instance to use for connecting to HBase.
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @param entityManager
   *          The EntityManager which will create the entity mapper that will
   *          power this dao.
   * @param entitySchemaString
   *          The schema as a string representing the schema version that this
   *          DAO should use for writes.
   */
  public GenericAvroDao(TransactionManager transactionManager,
      HTablePool tablePool, String tableName, String entityName,
      GenericAvroEntityManager entityManager, String entitySchemaString) {

    super(transactionManager, tablePool, tableName, entityManager
        .registerEntitySchema(tableName, entityName,
            parser.parseEntity(entitySchemaString)).createEntityMapper(
            tableName, entityName));
  }

  /**
   * Construct the GenericAvroDao with an EntityManager, which will provide the
   * entity mapper to this Dao that knows how to map the different entity schema
   * versions defined by the managed schema. The newest schema version available
   * at the time of this dao's creation will be used for writes.
   * 
   * @param transactionManager
   *          The TransactionManager that will manage transactional entities.
   * @param tablePool
   *          An HTabePool instance to use for connecting to HBase.
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @param entityManager
   *          The EntityManager which will create the entity mapper that will
   *          power this dao.
   */
  public GenericAvroDao(TransactionManager transactionManager,
      HTablePool tablePool, String tableName, String entityName,
      GenericAvroEntityManager entityManager) {

    super(transactionManager, tablePool, tableName, entityManager
        .registerEntitySchema(tableName, entityName,
            entityManager.getEntitySchema(tableName, entityName))
        .createEntityMapper(tableName, entityName));
  }

  private static BaseEntityMapper<GenericRecord, GenericRecord> buildEntityMapper(
      String readerSchemaStr, String keySchemaStr) {
    return buildEntityMapper(readerSchemaStr, readerSchemaStr, keySchemaStr);
  }

  private static BaseEntityMapper<GenericRecord, GenericRecord> buildEntityMapper(
      String readerSchemaStr, String writtenSchemaStr, String keySchemaStr) {

    AvroEntitySchema readerSchema = parser.parseEntity(readerSchemaStr);
    AvroEntitySchema writtenSchema = parser.parseEntity(writtenSchemaStr);
    AvroKeySchema keySchema = parser.parseKey(keySchemaStr);
    AvroKeySerDe<GenericRecord> keySerDe = new AvroKeySerDe<GenericRecord>(
        keySchema.getAvroSchema(), false);
    AvroEntityComposer<GenericRecord> entityComposer = new AvroEntityComposer<GenericRecord>(
        readerSchema, false);
    AvroEntitySerDe<GenericRecord> entitySerDe = new AvroEntitySerDe<GenericRecord>(
        entityComposer, readerSchema, writtenSchema, false);

    return new BaseEntityMapper<GenericRecord, GenericRecord>(keySchema,
        readerSchema, keySerDe, entitySerDe);
  }
}
