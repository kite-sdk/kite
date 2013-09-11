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
package com.cloudera.cdk.data.hbase.avro;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.data.dao.Dao;
import com.cloudera.cdk.data.dao.HBaseCommonException;
import com.cloudera.cdk.data.dao.KeyEntity;
import com.cloudera.cdk.data.dao.SchemaManager;
import com.cloudera.cdk.data.dao.SchemaNotFoundException;
import com.cloudera.cdk.data.dao.SchemaValidationException;
import com.cloudera.cdk.data.hbase.BaseDao;
import com.cloudera.cdk.data.hbase.BaseEntityMapper;
import com.cloudera.cdk.data.hbase.CompositeBaseDao;
import com.cloudera.cdk.data.hbase.EntityMapper;
import com.cloudera.cdk.data.hbase.avro.impl.AvroEntityComposer;
import com.cloudera.cdk.data.hbase.avro.impl.AvroEntitySchema;
import com.cloudera.cdk.data.hbase.avro.impl.AvroEntitySerDe;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeyEntitySchemaParser;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeySchema;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeySerDe;
import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
import com.cloudera.cdk.data.hbase.avro.impl.VersionedAvroEntityMapper;

/**
 * A Dao for Avro's SpecificRecords. In this Dao implementation, both the
 * underlying key record type, and the entity type are SpecificRecords. This Dao
 * allows us to persist and fetch these SpecificRecords to and from HBase.
 * 
 * @param <K>
 *          The Key's underlying record type.
 * @param <E>
 *          The entity type.
 */
public class SpecificAvroDao<K extends SpecificRecord, E extends SpecificRecord>
    extends BaseDao<K, E> {

  private static Logger LOG = LoggerFactory.getLogger(SpecificAvroDao.class);

  private static final AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();

  /**
   * Construct the SpecificAvroDao.
   * 
   * @param tablePool
   *          An HTablePool instance to use for connecting to HBase.
   * @param tableName
   *          The name of the table this Dao will read from and write to.
   * @param keySchemaString
   *          The Avro schema string that represents the Key structure for row
   *          keys in this table.
   * @param entitySchemaString
   *          The json string representing the special avro record schema, that
   *          contains metadata in annotations of the Avro record fields. See
   *          {@link AvroEntityMapper} for details.
   * @param keyClass
   *          The class of the SpecificRecord this DAO will use as a key
   * @param entityClass
   *          The class of the SpecificRecord this DAO will persist and fetch.
   */
  public SpecificAvroDao(HTablePool tablePool, String tableName,
      String keySchemaString, String entitySchemaString, Class<K> keyClass,
      Class<E> entityClass) {

    super(tablePool, tableName, buildEntityMapper(entitySchemaString,
        entitySchemaString, keySchemaString, keyClass, entityClass));
  }

  /**
   * Construct the SpecificAvroDao.
   * 
   * @param tablePool
   *          An HTablePool instance to use for connecting to HBase.
   * @param tableName
   *          The name of the table this Dao will read from and write to.
   * @param keySchemaStream
   *          The json stream representing the avro schema for the key.
   * @param entitySchemaStream
   *          The json stream representing the special avro record schema, that
   *          contains metadata in annotations of the Avro record fields. See
   *          {@link AvroEntityMapper} for details.
   * @param keyClass
   *          The class of the SpecificRecord this DAO will use as a key
   * @param entityClass
   *          The class of the SpecificRecord this DAO will persist and fetch.
   */
  public SpecificAvroDao(HTablePool tablePool, String tableName,
      InputStream keySchemaStream, InputStream entitySchemaStream,
      Class<K> keyClass, Class<E> entityClass) {

    this(tablePool, tableName, AvroUtils.inputStreamToString(keySchemaStream),
        AvroUtils.inputStreamToString(entitySchemaStream), keyClass,
        entityClass);
  }

  /**
   * Construct the SpecificAvroDao with an EntityManager, which will provide the
   * entity mapper to this Dao that knows how to map the different entity schema
   * versions defined by the managed schema.
   * 
   * @param tablePool
   *          An HTabePool instance to use for connecting to HBase.
   * @param tableName
   *          The table name of the managed schema.
   * @param entityName
   *          The entity name of the managed schema.
   * @param schemaManager
   *          The SchemaManager which will be used to query schema information
   *          from the meta store.
   */
  public SpecificAvroDao(HTablePool tablePool, String tableName,
      String entityName, SchemaManager schemaManager) {
    super(tablePool, tableName, new VersionedAvroEntityMapper.Builder()
        .setSchemaManager(schemaManager).setTableName(tableName)
        .setEntityName(entityName).setSpecific(true).<K, E> build());
  }

  /**
   * Create a CompositeDao, which will return SpecificRecord instances
   * represented by the entitySchemaString avro schema. This avro schema must be
   * a composition of the schemas in the subEntitySchemaStrings list.
   * 
   * @param tablePool
   *          An HTablePool instance to use for connecting to HBase
   * @param tableName
   *          The table name this dao will read from and write to
   * @param keySchemaString
   *          The Avro schema string that represents the Key structure for row
   *          keys in this table.
   * @param subEntitySchemaStrings
   *          The list of entities that make up the composite. This list must be
   *          in the same order as the fields defined in the entitySchemaString.
   * @param keyClass
   *          The class of the SpecificRecord representing the Key of rows this
   *          dao will fetch.
   * @param entityClass
   *          The class of the SpecificRecord this DAO will persist and fetch.
   * @return The CompositeDao instance.
   * @throws SchemaNotFoundException
   * @throws SchemaValidationException
   */
  @SuppressWarnings("unchecked")
  public static <K extends SpecificRecord, E extends SpecificRecord, S extends SpecificRecord> Dao<K, E> buildCompositeDao(
      HTablePool tablePool, String tableName, String keySchemaString,
      List<String> subEntitySchemaStrings, Class<K> keyClass,
      Class<E> entityClass) {

    List<EntityMapper<K, S>> entityMappers = new ArrayList<EntityMapper<K, S>>();
    for (String subEntitySchemaString : subEntitySchemaStrings) {
      AvroEntitySchema subEntitySchema = parser
          .parseEntity(subEntitySchemaString);
      Class<S> subEntityClass;
      try {
        subEntityClass = (Class<S>) Class.forName(subEntitySchema
            .getAvroSchema().getFullName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      entityMappers.add(SpecificAvroDao.<K, S> buildEntityMapper(
          subEntitySchemaString, subEntitySchemaString, keySchemaString,
          keyClass, subEntityClass));
    }

    return new SpecificCompositeAvroDao<K, E, S>(tablePool, tableName,
        entityMappers, entityClass);
  }

  /**
   * Create a CompositeDao, which will return SpecificRecord instances
   * represented by the entitySchemaString avro schema. This avro schema must be
   * a composition of the schemas in the subEntitySchemaStrings list.
   * 
   * @param tablePool
   *          An HTablePool instance to use for connecting to HBase
   * @param tableName
   *          The table name this dao will read from and write to
   * @param keySchemaStream
   *          The Avro schema input stream that represents the Key structure for
   *          row keys in this table.
   * @param subEntitySchemaStreams
   *          The list of entities that make up the composite. This list must be
   *          in the same order as the fields defined in the entitySchemaString.
   * @param keyClass
   *          The class of the SpecificRecord representing the Key of rows this
   *          dao will fetch.
   * @param entityClass
   *          The class of the SpecificRecord this DAO will persist and fetch.
   * @return The CompositeDao instance.
   * @throws SchemaNotFoundException
   * @throws SchemaValidationException
   */
  public static <K extends SpecificRecord, E extends SpecificRecord, S extends SpecificRecord> Dao<K, E> buildCompositeDaoWithInputStream(
      HTablePool tablePool, String tableName, InputStream keySchemaStream,
      List<InputStream> subEntitySchemaStreams, Class<K> keyClass,
      Class<E> entityClass) {

    List<String> subEntitySchemaStrings = new ArrayList<String>();
    for (InputStream subEntitySchemaStream : subEntitySchemaStreams) {
      subEntitySchemaStrings.add(AvroUtils
          .inputStreamToString(subEntitySchemaStream));
    }
    return buildCompositeDao(tablePool, tableName,
        AvroUtils.inputStreamToString(keySchemaStream), subEntitySchemaStrings,
        keyClass, entityClass);
  }

  /**
   * Create a CompositeDao, which will return SpecificRecord instances
   * represented by the entitySchemaString avro schema. This avro schema must be
   * a composition of the schemas in the subEntitySchemaStrings list.
   * 
   * @param tablePool
   *          An HTabePool instance to use for connecting to HBase.
   * @param tableName
   *          The table name of the managed schema.
   * @param entityClass
   *          The class that is the composite record, which is made up of fields
   *          referencing the sub records.
   * @param schemaManager
   *          The SchemaManager which will use to create the entity mapper that
   *          will power this dao.
   * @return The CompositeDao instance.
   * @throws SchemaNotFoundException
   */
  public static <K extends SpecificRecord, E extends SpecificRecord, S extends SpecificRecord> Dao<K, E> buildCompositeDaoWithEntityManager(
      HTablePool tablePool, String tableName, Class<E> entityClass,
      SchemaManager schemaManager) {

    Schema entitySchema;
    try {
      entitySchema = (Schema) entityClass.getDeclaredField("SCHEMA$").get(null);
    } catch (Throwable e) {
      LOG.error(
          "Error getting schema from entity of type: " + entityClass.getName(),
          e);
      throw new HBaseCommonException(e);
    }

    List<EntityMapper<K, S>> entityMappers = new ArrayList<EntityMapper<K, S>>();
    for (Schema.Field field : entitySchema.getFields()) {
      entityMappers.add(new VersionedAvroEntityMapper.Builder()
          .setSchemaManager(schemaManager).setTableName(tableName)
          .setEntityName(field.schema().getName()).setSpecific(true)
          .<K, S> build());
    }

    return new SpecificCompositeAvroDao<K, E, S>(tablePool, tableName,
        entityMappers, entityClass);
  }

  /**
   * CompositeBaseDao implementation for Specific avro records.
   * 
   * @param <K>
   *          The key type this dao fetches and persists
   * @param <E>
   *          The entity type this dao fetches and persists
   */
  private static class SpecificCompositeAvroDao<K extends SpecificRecord, E extends SpecificRecord, S extends SpecificRecord>
      extends CompositeBaseDao<K, E, S> {

    private final Class<E> entityClass;
    private final Constructor<E> entityConstructor;
    private final Schema entitySchema;

    public SpecificCompositeAvroDao(HTablePool tablePool, String tableName,
        List<EntityMapper<K, S>> entityMappers, Class<E> entityClass) {

      super(tablePool, tableName, entityMappers);
      this.entityClass = entityClass;
      try {
        entityConstructor = entityClass.getConstructor();
        entitySchema = (Schema) entityClass.getDeclaredField("SCHEMA$").get(
            null);
      } catch (Throwable e) {
        LOG.error(
            "Error getting constructor or schema field for entity of type: "
                + entityClass.getName(), e);
        throw new HBaseCommonException(e);
      }
    }

    @Override
    public KeyEntity<K, E> compose(List<KeyEntity<K, S>> keyEntities) {
      K key = null;
      E entity;
      try {
        entity = entityConstructor.newInstance();
      } catch (Throwable e) {
        LOG.error(
            "Error trying to construct entity of type: "
                + entityClass.getName(), e);
        throw new HBaseCommonException(e);
      }

      int cnt = 0;
      for (KeyEntity<K, S> keyEntity : keyEntities) {
        if (keyEntity != null) {
          if (key == null) {
            key = keyEntity.getKey();
          }
          entity.put(cnt, keyEntity.getEntity());
        }
        cnt++;
      }
      return new KeyEntity<K, E>(key, entity);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<S> decompose(E entity) {
      List<S> subEntityList = new ArrayList<S>();
      for (int i = 0; i < entitySchema.getFields().size(); i++) {
        subEntityList.add((S) entity.get(i));
      }
      return subEntityList;
    }
  }

  private static <K extends SpecificRecord, E extends SpecificRecord> BaseEntityMapper<K, E> buildEntityMapper(
      String readerSchemaStr, String writtenSchemaStr, String keySchemaStr,
      Class<K> keyClass, Class<E> entityClass) {

    AvroEntitySchema readerSchema = parser.parseEntity(readerSchemaStr);
    // The specific class may have been compiled with a setting that adds the
    // string
    // type to the string fields, but aren't in the local or managed schemas.
    readerSchema = AvroUtils
        .mergeSpecificStringTypes(entityClass, readerSchema);
    AvroEntitySchema writtenSchema = parser.parseEntity(writtenSchemaStr);
    AvroEntityComposer<E> entityComposer = new AvroEntityComposer<E>(
        readerSchema, true);
    AvroEntitySerDe<E> entitySerDe = new AvroEntitySerDe<E>(entityComposer,
        readerSchema, writtenSchema, true);

    AvroKeySchema keySchema = parser.parseKey(keySchemaStr);
    // Same as above. The key schema passed may have a different String type.
    keySchema = AvroUtils.mergeSpecificStringTypes(keyClass, keySchema);
    AvroKeySerDe<K> keySerDe = new AvroKeySerDe<K>(keySchema.getAvroSchema(),
        true);

    return new BaseEntityMapper<K, E>(keySchema, readerSchema, keySerDe,
        entitySerDe);
  }
}
