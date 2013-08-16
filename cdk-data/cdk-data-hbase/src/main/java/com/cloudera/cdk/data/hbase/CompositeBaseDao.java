// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;

import com.cloudera.cdk.data.hbase.EntitySchema.FieldMapping;

/**
 * Base implementation of the CompositeDao interface. Internally managed
 * multiple EntityMappers that each handle their respective sub types.
 * 
 * @param <K>
 *          The type of the key
 * @param <E>
 *          The type of the entity this DAO returns. This entity will be a
 *          composition of the sub entities.
 * @param <S>
 *          The type of the sub entities.
 */
public abstract class CompositeBaseDao<K, E, S> implements
    CompositeDao<K, E, S> {

  private final BaseDao<K, E> baseDao;

  /**
   * An EntityMapper implementation that will map to and from multiple entities
   * per row. These are the sub entities that make up the composed entity.
   */
  private class CompositeEntityMapper implements EntityMapper<K, E> {

    private final List<EntityMapper<K, S>> entityMappers;

    public CompositeEntityMapper(List<EntityMapper<K, S>> entityMappers) {
      if (entityMappers.size() == 0) {
        throw new IllegalArgumentException(
            "Must provide more than one entity mapper to CompositeEntityMapper");
      }
      this.entityMappers = entityMappers;
    }

    @Override
    public KeyEntity<K, E> mapToEntity(Result result) {
      List<KeyEntity<K, S>> entityList = new ArrayList<KeyEntity<K, S>>();
      for (EntityMapper<K, S> entityMapper : entityMappers) {
        KeyEntity<K, S> keyEntity = entityMapper.mapToEntity(result);
        // could be null. compose will handle a null sub entity appropriately.
        entityList.add(keyEntity);
      }
      return compose(entityList);
    }

    @Override
    public PutAction mapFromEntity(K key, E entity) {
      List<PutAction> puts = new ArrayList<PutAction>();
      List<S> subEntities = decompose(entity);
      for (int i = 0; i < entityMappers.size(); i++) {
        S subEntity = subEntities.get(i);
        if (subEntity != null) {
          puts.add(entityMappers.get(i).mapFromEntity(key, subEntity));
        }
      }
      @SuppressWarnings("unchecked")
      byte[] keyBytes = getKeySerDe().serialize(key);
      return HBaseUtils.mergePutActions(keyBytes, puts);
    }

    @Override
    public Increment mapToIncrement(K key, String fieldName, long amount) {
      throw new UnsupportedOperationException(
          "We don't currently support increment on CompositeDaos");
    }

    @Override
    public long mapFromIncrementResult(Result result, String fieldName) {
      throw new UnsupportedOperationException(
          "We don't currently support increment on CompositeDaos");
    }

    @Override
    public Set<String> getRequiredColumns() {
      Set<String> requiredColumnsSet = new HashSet<String>();
      for (EntityMapper<K, ?> entityMapper : entityMappers) {
        requiredColumnsSet.addAll(entityMapper.getRequiredColumns());
      }
      return requiredColumnsSet;
    }

    @Override
    public Set<String> getRequiredColumnFamilies() {
      Set<String> requiredColumnFamiliesSet = new HashSet<String>();
      for (EntityMapper<K, ?> entityMapper : entityMappers) {
        requiredColumnFamiliesSet.addAll(entityMapper
            .getRequiredColumnFamilies());
      }
      return requiredColumnFamiliesSet;
    }

    @Override
    public KeySchema<?> getKeySchema() {
      return entityMappers.get(0).getKeySchema();
    }

    @Override
    public EntitySchema<?> getEntitySchema() {
      boolean transactional = entityMappers.get(0).getEntitySchema()
          .isTransactional();
      List<String> tables = new ArrayList<String>();
      List<FieldMapping> fieldMappings = new ArrayList<FieldMapping>();
      for (EntityMapper<K, ?> entityMapper : entityMappers) {
        tables.addAll(entityMapper.getEntitySchema().getTables());
        fieldMappings.addAll(entityMapper.getEntitySchema().getFieldMappings());
      }
      return new EntitySchema<Object>(tables, null, fieldMappings,
          transactional);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public KeySerDe getKeySerDe() {
      return entityMappers.get(0).getKeySerDe();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public EntitySerDe getEntitySerDe() {
      return entityMappers.get(0).getEntitySerDe();
    }
  }

  /**
   * Constructor that will internally create an HBaseClientTemplate from the
   * tablePool and the tableName.
   * 
   * @param tablePool
   *          A pool of HBase Tables.
   * @param tableName
   *          The name of the table this dao persists to and fetches from.
   * @param entityMappers
   *          Maps between entities and the HBase operations for their
   *          respective sub entities.
   */
  public CompositeBaseDao(HTablePool tablePool, String tableName,
      List<EntityMapper<K, S>> entityMappers) {
    baseDao = new BaseDao<K, E>(tablePool, tableName,
        new CompositeEntityMapper(entityMappers));
  }

  @Override
  public E get(K key) {
    return baseDao.get(key);
  }

  @Override
  public boolean put(K key, E entity) {
    return baseDao.put(key, entity);
  }

  @Override
  public long increment(K key, String fieldName, long amount) {
    throw new UnsupportedOperationException(
        "We don't currently support increment on CompositeDaos");
  }

  @Override
  public void delete(K key) {
    baseDao.delete(key);
  }

  @Override
  public boolean delete(K key, E entity) {
    return baseDao.delete(key, entity);
  }

  @Override
  public EntityScanner<K, E> getScanner() {
    return baseDao.getScanner();
  }

  @Override
  public EntityScanner<K, E> getScanner(K startKey, K stopKey) {
    return baseDao.getScanner(startKey, stopKey);
  }

  @Override
  public EntityScanner<K, E> getScanner(PartialKey<K> startKey,
      PartialKey<K> stopKey) {
    return baseDao.getScanner(startKey, stopKey);
  }

  @Override
  public EntityScanner<K, E> getScanner(K startKey, K stopKey,
      ScanModifier scanModifier) {
    return baseDao.getScanner(startKey, stopKey, scanModifier);
  }

  @Override
  public EntityScanner<K, E> getScanner(PartialKey<K> startKey,
      PartialKey<K> stopKey, ScanModifier scanModifier) {
    return baseDao.getScanner(startKey, stopKey, scanModifier);
  }

  @Override
  public EntityScannerBuilder<K, E> getScannerBuilder() {
    return baseDao.getScannerBuilder();
  }

  @Override
  public KeySchema<?> getKeySchema() {
    return baseDao.getKeySchema();
  }

  @Override
  public EntitySchema<?> getEntitySchema() {
    return baseDao.getEntitySchema();
  }

  @Override
  public KeySerDe<K> getKeySerDe() {
    return baseDao.getKeySerDe();
  }

  @Override
  public EntitySerDe<E> getEntitySerDe() {
    return baseDao.getEntitySerDe();
  }

  @Override
  public EntityBatch<K, E> newBatch(long writeBufferSize) {
    return baseDao.newBatch(writeBufferSize);
  }

  @Override
  public EntityBatch<K, E> newBatch() {
    return baseDao.newBatch();
  }

  public EntityMapper<K, E> getEntityMapper() {
    return baseDao.getEntityMapper();
  }
}
