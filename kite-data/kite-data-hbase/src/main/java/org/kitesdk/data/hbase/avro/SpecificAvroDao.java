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
package org.kitesdk.data.hbase.avro;

import org.kitesdk.data.DatasetException;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.hbase.impl.BaseDao;
import org.kitesdk.data.hbase.impl.BaseEntityMapper;
import org.kitesdk.data.hbase.impl.CompositeBaseDao;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.EntityMapper;
import org.kitesdk.data.hbase.impl.SchemaManager;
import com.google.common.collect.Lists;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Dao for Avro's SpecificRecords. In this Dao implementation, both the
 * underlying key record type, and the entity type are SpecificRecords. This Dao
 * allows us to persist and fetch these SpecificRecords to and from HBase.
 * 
 * @param <E>
 *          The entity type.
 */
public class SpecificAvroDao<E extends SpecificRecord> extends BaseDao<E> {

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
   *          The Avro schema string that represents the StorageKey structure for row
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
      String entitySchemaString, Class<E> entityClass) {

    super(tablePool, tableName, buildEntityMapper(entitySchemaString,
        entitySchemaString, entityClass));
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
      InputStream entitySchemaStream, Class<E> entityClass) {

    this(tablePool, tableName, AvroUtils
        .inputStreamToString(entitySchemaStream), entityClass);
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
        .setEntityName(entityName).setSpecific(true).<E> build());
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
   *          The Avro schema string that represents the StorageKey structure for row
   *          keys in this table.
   * @param subEntitySchemaStrings
   *          The list of entities that make up the composite. This list must be
   *          in the same order as the fields defined in the entitySchemaString.
   * @param keyClass
   *          The class of the SpecificRecord representing the StorageKey of rows this
   *          dao will fetch.
   * @param entityClass
   *          The class of the SpecificRecord this DAO will persist and fetch.
   * @return The CompositeDao instance.
   * @throws SchemaNotFoundException
   * @throws ValidationException
   */
  @SuppressWarnings("unchecked")
  public static <E extends SpecificRecord, S extends SpecificRecord> Dao<E> buildCompositeDao(
      HTablePool tablePool, String tableName,
      List<String> subEntitySchemaStrings, Class<E> entityClass) {

    List<EntityMapper<S>> entityMappers = new ArrayList<EntityMapper<S>>();
    for (String subEntitySchemaString : subEntitySchemaStrings) {
      AvroEntitySchema subEntitySchema = parser
          .parseEntitySchema(subEntitySchemaString);
      Class<S> subEntityClass;
      try {
        subEntityClass = (Class<S>) Class.forName(subEntitySchema
            .getAvroSchema().getFullName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      entityMappers.add(SpecificAvroDao.<S> buildEntityMapper(
          subEntitySchemaString, subEntitySchemaString, subEntityClass));
    }

    return new SpecificCompositeAvroDao<E, S>(tablePool, tableName,
        entityMappers, entityClass);
  }

  /**
   * Create a CompositeDao, which will return SpecificRecord instances
   * in a Map container.
   *
   * @param tablePool
   *          An HTablePool instance to use for connecting to HBase
   * @param tableName
   *          The table name this dao will read from and write to
   * @param keySchemaString
   *          The Avro schema string that represents the StorageKey structure for row
   *          keys in this table.
   * @param subEntitySchemaStrings
   *          The list of entities that make up the composite.
   * @param keyClass
   *          The class of the SpecificRecord representing the StorageKey of rows this
   *          dao will fetch.
   * @return The CompositeDao instance.
   * @throws SchemaNotFoundException
   * @throws ValidationException
   */
  @SuppressWarnings("unchecked")
  public static <K extends SpecificRecord, S extends SpecificRecord> Dao<
      Map<String, S>> buildCompositeDao(
      HTablePool tablePool, String tableName,
      List<String> subEntitySchemaStrings) {

    List<EntityMapper<S>> entityMappers = new ArrayList<EntityMapper<S>>();
    for (String subEntitySchemaString : subEntitySchemaStrings) {
      AvroEntitySchema subEntitySchema = parser
          .parseEntitySchema(subEntitySchemaString);
      Class<S> subEntityClass;
      try {
        subEntityClass = (Class<S>) Class.forName(subEntitySchema
            .getAvroSchema().getFullName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      entityMappers.add(SpecificAvroDao.<S> buildEntityMapper(
          subEntitySchemaString, subEntitySchemaString, 
          subEntityClass));
    }

    return new SpecificMapCompositeAvroDao<S>(tablePool, tableName, entityMappers);
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
   *          The Avro schema input stream that represents the StorageKey structure for
   *          row keys in this table.
   * @param subEntitySchemaStreams
   *          The list of entities that make up the composite. This list must be
   *          in the same order as the fields defined in the entitySchemaString.
   * @param keyClass
   *          The class of the SpecificRecord representing the StorageKey of rows this
   *          dao will fetch.
   * @param entityClass
   *          The class of the SpecificRecord this DAO will persist and fetch.
   * @return The CompositeDao instance.
   * @throws SchemaNotFoundException
   * @throws ValidationException
   */
  public static <E extends SpecificRecord, S extends SpecificRecord> Dao<E> buildCompositeDaoWithInputStream(
      HTablePool tablePool, String tableName,
      List<InputStream> subEntitySchemaStreams, Class<E> entityClass) {

    List<String> subEntitySchemaStrings = new ArrayList<String>();
    for (InputStream subEntitySchemaStream : subEntitySchemaStreams) {
      subEntitySchemaStrings.add(AvroUtils
          .inputStreamToString(subEntitySchemaStream));
    }
    return buildCompositeDao(tablePool, tableName, subEntitySchemaStrings,
        entityClass);
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
  public static <K extends SpecificRecord, E extends SpecificRecord, S extends SpecificRecord> Dao<E> buildCompositeDaoWithEntityManager(
      HTablePool tablePool, String tableName, Class<E> entityClass,
      SchemaManager schemaManager) {

    Schema entitySchema = getSchemaFromEntityClass(entityClass);

    List<EntityMapper<S>> entityMappers = new ArrayList<EntityMapper<S>>();
    for (Schema.Field field : entitySchema.getFields()) {
      entityMappers.add(new VersionedAvroEntityMapper.Builder()
          .setSchemaManager(schemaManager).setTableName(tableName)
          .setEntityName(getSchemaName(field.schema())).setSpecific(true)
          .<S> build());
    }

    return new SpecificCompositeAvroDao<E, S>(tablePool, tableName,
        entityMappers, entityClass);
  }

  private static String getSchemaName(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      List<Schema> types = schema.getTypes();
      if (types.size() == 2) {
        if (types.get(0).getType() == Schema.Type.NULL) {
          return types.get(1).getName();
        } else if (types.get(1).getType() == Schema.Type.NULL) {
          return types.get(0).getName();
        }
      }
      throw new IllegalArgumentException("Unsupported union schema: " + schema);
    }
    return schema.getName();
  }

  /**
   * Create a CompositeDao, which will return SpecificRecord instances
   * in a Map container.
   *
   * @param tablePool
   *          An HTablePool instance to use for connecting to HBase.
   * @param tableName
   *          The table name of the managed schema.
   * @param subEntityClasses
   *          The classes that make up the subentities.
   * @param schemaManager
   *          The SchemaManager which will use to create the entity mapper that
   *          will power this dao.
   * @return The CompositeDao instance.
   * @throws SchemaNotFoundException
   */
  public static <K extends SpecificRecord, S extends SpecificRecord> Dao<Map<String, S>> buildCompositeDaoWithEntityManager(
      HTablePool tablePool, String tableName, List<Class<S>> subEntityClasses,
      SchemaManager schemaManager) {

    List<EntityMapper<S>> entityMappers = new ArrayList<EntityMapper<S>>();
    for (Class<S> subEntityClass : subEntityClasses) {
      String entityName = getSchemaFromEntityClass(subEntityClass).getName();
      entityMappers.add(new VersionedAvroEntityMapper.Builder()
          .setSchemaManager(schemaManager).setTableName(tableName)
          .setEntityName(entityName).setSpecific(true)
          .<S> build());
    }

    return new SpecificMapCompositeAvroDao<S>(tablePool, tableName,
        entityMappers);
  }

  private static Schema getSchemaFromEntityClass(Class<?> entityClass) {
    try {
      return (Schema) entityClass.getDeclaredField("SCHEMA$").get(null);
    } catch (Throwable e) {
      LOG.error(
          "Error getting schema from entity of type: " + entityClass.getName(),
          e);
      throw new DatasetException(e);
    }
  }

  /**
   * CompositeBaseDao implementation for Specific avro records.
   * 
   * @param <K>
   *          The key type this dao fetches and persists
   * @param <E>
   *          The entity type this dao fetches and persists
   */
  private static class SpecificCompositeAvroDao<E extends SpecificRecord, S extends SpecificRecord>
      extends CompositeBaseDao<E, S> {

    private final Class<E> entityClass;
    private final Constructor<E> entityConstructor;
    private final Schema entitySchema;

    public SpecificCompositeAvroDao(HTablePool tablePool, String tableName,
        List<EntityMapper<S>> entityMappers, Class<E> entityClass) {

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
        throw new DatasetException(e);
      }
    }

    @Override
    public E compose(List<S> subEntities) {
      E entity;
      try {
        entity = entityConstructor.newInstance();
      } catch (Throwable e) {
        LOG.error(
            "Error trying to construct entity of type: "
                + entityClass.getName(), e);
        throw new DatasetException(e);
      }

      int cnt = 0;
      for (S subEntity : subEntities) {
        if (subEntity != null) {
          entity.put(cnt, subEntity);
        }
        cnt++;
      }
      return entity;
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

  /**
   * CompositeBaseDao implementation for Specific avro records where the composite
   * entity is a map.
   */
  private static class SpecificMapCompositeAvroDao<
      S extends SpecificRecord>
      extends CompositeBaseDao<Map<String, S>, S> {

    private final List<Schema> subEntitySchemas;

    public SpecificMapCompositeAvroDao(HTablePool tablePool, String tableName,
        List<EntityMapper<S>> entityMappers) {

      super(tablePool, tableName, entityMappers);
      subEntitySchemas = Lists.newArrayList();
      for (EntityMapper<S> entityMapper : entityMappers) {
        subEntitySchemas.add(parser.parseEntitySchema(entityMapper.getEntitySchema().getRawSchema()).getAvroSchema());
      }
    }

    @Override
    public Map<String, S> compose(List<S> entities) {
      Map<String, S> retEntity = new HashMap<String, S>();
      int cnt = 0;
      for (S entity : entities) {
        if (entity != null) {
          retEntity.put(subEntitySchemas.get(cnt).getName(), entity);
        }
        cnt++;
      }
      return retEntity;
    }

    @Override
    public List<S> decompose(Map<String, S> entity) {
      List<S> subEntityList = new ArrayList<S>();
      for (Schema s : subEntitySchemas) {
        subEntityList.add(entity.get(s.getName()));
      }
      return subEntityList;
    }
  }

  private static <E extends SpecificRecord> BaseEntityMapper<E> buildEntityMapper(
      String readerSchemaStr, String writtenSchemaStr,
      Class<E> entityClass) {

    AvroEntitySchema readerSchema = parser.parseEntitySchema(readerSchemaStr);
    // The specific class may have been compiled with a setting that adds the
    // string type to the string fields, but aren't in the local or managed
    // schemas.
    readerSchema = AvroUtils
        .mergeSpecificStringTypes(entityClass, readerSchema);
    AvroEntitySchema writtenSchema = parser.parseEntitySchema(writtenSchemaStr);
    AvroEntityComposer<E> entityComposer = new AvroEntityComposer<E>(
        readerSchema, true);
    AvroEntitySerDe<E> entitySerDe = new AvroEntitySerDe<E>(entityComposer,
        readerSchema, writtenSchema, true);

    AvroKeySchema keySchema = parser.parseKeySchema(readerSchemaStr);
    keySchema = AvroUtils.mergeSpecificStringTypes(entityClass, keySchema);
    AvroKeySerDe keySerDe = new AvroKeySerDe(keySchema.getAvroSchema(),
        keySchema.getPartitionStrategy());

    return new BaseEntityMapper<E>(keySchema, readerSchema, keySerDe,
        entitySerDe);
  }
}
