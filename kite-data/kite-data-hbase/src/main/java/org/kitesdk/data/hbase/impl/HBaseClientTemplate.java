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

import com.google.common.collect.ImmutableList;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.PartitionKey;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class simplifies the use of HBase client, and helps to avoid common
 * errors by managing its own cleanup of resources.
 * 
 * This class uses the Template Method pattern to allow users to implement
 * callbacks for things like entity mapping, and get and put modification
 * (modify Get and Put before sending them off to HBase).
 */
public class HBaseClientTemplate {

  private final HTablePool pool;
  private final String tableName;

  private final List<GetModifier> getModifiers = new ArrayList<GetModifier>();
  private final List<PutActionModifier> putActionModifiers = new ArrayList<PutActionModifier>();
  private final List<DeleteActionModifier> deleteActionModifiers = new ArrayList<DeleteActionModifier>();
  private final List<ScanModifier> scanModifiers = new ArrayList<ScanModifier>();

  /**
   * Construct an HBaseClientTemplate. Requires an HTablePool to acquire HBase
   * connections from, and the name of the table to interact with.
   * 
   * @param pool
   *          The pool of HBase connections.
   * @param tableName
   *          The name of the table to interact with.
   */
  public HBaseClientTemplate(HTablePool pool, String tableName) {
    this.pool = pool;
    this.tableName = tableName;
  }

  /**
   * Creates a client template that is a copy of an existing client template.
   * 
   * @param clientTemplate
   */
  public HBaseClientTemplate(HBaseClientTemplate clientTemplate) {
    this.pool = clientTemplate.pool;
    this.tableName = clientTemplate.tableName;

    this.getModifiers.addAll(clientTemplate.getModifiers);
    this.putActionModifiers.addAll(clientTemplate.putActionModifiers);
    this.deleteActionModifiers.addAll(clientTemplate.deleteActionModifiers);
    this.scanModifiers.addAll(clientTemplate.scanModifiers);
  }

  /**
   * Register a GetModifier to be called before every Get is executed on HBase.
   * This GetModifier will be replaced if already registered and added if not.
   * Equality is checked by calling the equals() on the getModifier passed.
   * GetModifiers will be called in the order they are added to the
   * template, so if any modifier is destructive, it must be added in the right
   * order.
   *
   * Not Thread Safe
   * @param getModifier The GetModifier to register.
   */
  public void registerGetModifier(GetModifier getModifier) {
    int currentIndex = getModifiers.indexOf(getModifier);
    if(currentIndex == -1) {
      getModifiers.add(getModifier);
    } else {
      getModifiers.set(currentIndex, getModifier);
    }
  }

  /**
   * Returns a list of Get Modifiers currently registered
   *
   * @return List of GetModifier
   */
  public List<GetModifier> getGetModifiers() {
    return ImmutableList.copyOf(getModifiers);
  }

  /**
   * Register a PutActionModifier to be called before every Put is executed on
   * HBase. This PutActionModifier will be replaced if already registered and
   * added if not. Equality is checked by calling the equals() on the
   * putActionModifier passed. PutActionModifiers will be called in the order
   * they are added, so if any modifier is destructive,
   * it must be added in the right order.
   *
   * Not Thread Safe
   * @param putActionModifier The PutActionModifier to register.
   */
  public void registerPutActionModifier(PutActionModifier putActionModifier) {
    int currentIndex = putActionModifiers.indexOf(putActionModifier);
    if(currentIndex == -1) {
      putActionModifiers.add(putActionModifier);
    } else {
      putActionModifiers.set(currentIndex, putActionModifier);
    }
  }

  /**
   * Returns a list of Put Modifiers currently registered
   *
   * @return List of PutActionModifier
   */
  public List<PutActionModifier> getPutActionModifiers() {
    return ImmutableList.copyOf(putActionModifiers);
  }

  @VisibleForTesting
  public int countPutActionModifiers() {
    return putActionModifiers.size();
  }

  /**
   * Register a DeleteActionModifier to be called before every Delete is
   * executed on HBase. This DeleteActionModifier will be replaced if already
   * registered and added if its not. Equality is checked by calling the equals()
   * on the deleteActionModifier passed. DeleteActionModifiers will be called
   * in the order they are added, so if any modifier is destructive,
   * it must be added in the right order.
   *
   * Not Thread Safe
   * @param deleteActionModifier The DeleteActionModifier to register.
   */
  public void registerDeleteModifier(DeleteActionModifier deleteActionModifier) {
    int currentIndex = deleteActionModifiers.indexOf(deleteActionModifier);
    if(currentIndex == -1) {
      deleteActionModifiers.add(deleteActionModifier);
    } else {
      deleteActionModifiers.set(currentIndex, deleteActionModifier);
    }
  }

  /**
   * Returns a list of Delete Modifiers currently registered
   *
   * @return List of DeleteActionModifier
   */
  public List<DeleteActionModifier> getDeleteActionModifiers() {
    return ImmutableList.copyOf(deleteActionModifiers);
  }

  /**
   * Register a ScanModifier to be called before every Scan is executed on
   * HBase. This ScanModifier will be replaced if already registered and added
   * if not. Equality is checked by calling the equals() on the scanModifier passed.
   * ScanModifiers will be called in the order they are added,
   * so if any modifier is destructive, it must be added in the
   * right order.
   *
   * Not Thread Safe
   * @param scanModifier The ScanModifier to register.
   */
  public void registerScanModifier(ScanModifier scanModifier) {
    int currentIndex = scanModifiers.indexOf(scanModifier);
    if(currentIndex == -1) {
      scanModifiers.add(scanModifier);
    } else {
      scanModifiers.set(currentIndex, scanModifier);
    }
  }

  /**
   * Returns a list of Scan Modifiers currently registered
   *
   * @return List of ScanModifier
   */
  public List<ScanModifier> getScanModifiers() {
    return ImmutableList.copyOf(scanModifiers);
  }

  /**
   * Clear all GetModifiers registered with registerGetModifier.
   */
  public void clearGetModifiers() {
    getModifiers.clear();
  }

  /**
   * Clear all PutActionModifiers registered with registerPutActionModifier.
   */
  public void clearPutActionModifiers() {
    putActionModifiers.clear();
  }

  /**
   * Clear all DeleteActionModifiers registered with
   * registerDeleteActionModifier.
   */
  public void clearDeleteActionModifiers() {
    deleteActionModifiers.clear();
  }

  /**
   * Clear all ScanModifiers registered with registerScanModifier.
   */
  public void clearScanModifiers() {
    scanModifiers.clear();
  }

  /**
   * Clear all modifiers registered with the template for all operations.
   */
  public void clearAllModifiers() {
    clearGetModifiers();
    clearPutActionModifiers();
    clearDeleteActionModifiers();
    clearScanModifiers();
  }

  /**
   * Returns the table name
   * 
   * @return The table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Execute a Get on HBase.
   * 
   * Any GetModifers registered with registerGetModifier will be invoked before
   * the Get is executed.
   * 
   * @param get
   *          The Get to execute
   * @return Result returned from the Get.
   */
  public Result get(Get get) {
    HTableInterface table = pool.getTable(tableName);
    try {
      for (GetModifier getModifier : getModifiers) {
        get = getModifier.modifyGet(get);
      }
      try {
        return table.get(get);
      } catch (IOException e) {
        throw new DatasetIOException("Error performing get", e);
      }
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          throw new DatasetIOException("Error putting table back into pool", e);
        }
      }
    }
  }

  /**
   * Execute the get on HBase, invoking the getModifier before executing the get
   * if getModifier is not null.
   * 
   * Any GetModifers registered with registerGetModifier will be invoked before
   * the Get is executed, and after the getModifier passed to this function are
   * called.
   * 
   * @param get
   *          The Get to execute.
   * @param getModifier
   *          Invoked before the Get to give callers a chance to modify the Get
   *          before it is executed.
   * @return Result returned from the Get.
   */
  public Result get(Get get, GetModifier getModifier) {
    if (getModifier != null) {
      get = getModifier.modifyGet(get);
    }
    return get(get);
  }

  /**
   * Execute a Get on HBase, creating the Get from the key's toByteArray method.
   * The returned Result of the Get will be mapped to an entity with the
   * entityMapper, and that entity will be returned.
   * 
   * Any GetModifers registered with registerGetModifier will be invoked before
   * the Get is executed.
   * 
   * @param key
   *          The StorageKey to create a Get from.
   * @param entityMapper
   *          The EntityMapper to use to map the Result to an entity to return.
   * @return The entity created by the entityMapper.
   */
  public <E> E get(PartitionKey key, EntityMapper<E> entityMapper) {
    return get(key, null, entityMapper);
  }

  /**
   * Execute a Get on HBase, creating the Get from the key's toByteArray method.
   * The returned Result of the Get will be mapped to an entity with the
   * entityMapper, and that entity will be returned.
   * 
   * If the getModifier is not null, it will be invoked before the created Get
   * is executed.
   * 
   * Any GetModifers registered with registerGetModifier will be invoked after
   * the getModifier passed to this method is invoked, and before the Get is
   * executed.
   * 
   * @param key
   *          The StorageKey to create a Get from.
   * @param getModifier
   *          Invoked before the Get to give callers a chance to modify the Get
   *          before it is executed.
   * @param entityMapper
   *          The EntityMapper to use to map the Result to an entity to return.
   * @return The entity created by the entityMapper.
   */
  public <E> E get(PartitionKey key, GetModifier getModifier,
      EntityMapper<E> entityMapper) {
    byte[] keyBytes = entityMapper.getKeySerDe().serialize(key);
    Get get = new Get(keyBytes);
    HBaseUtils.addColumnsToGet(entityMapper.getRequiredColumns(), get);
    Result result = get(get, getModifier);
    if (result.isEmpty()) {
      return null;
    } else {
      return entityMapper.mapToEntity(result);
    }
  }

  /**
   * Execute a Put on HBase.
   * 
   * Any PutModifers registered with registerPutModifier will be invoked before
   * the Put is executed.
   * 
   * @param putAction
   *          The put to execute on HBase.
   * @return True if the put succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean put(PutAction putAction) {
    HTableInterface table = pool.getTable(tableName);
    try {
      return put(putAction, table);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          throw new DatasetIOException("Error putting table back into pool", e);
        }
      }
    }
  }

  /**
   * Execute a Put on HBase using a pre-define HTableInterface
   * 
   * Any PutModifers registered with registerPutModifier will be invoked before
   * the Put is executed.
   * 
   * @param putAction
   *          The put to execute on HBase.
   * @param table
   *          The HTableInterface object to interface with
   * @return True if the put succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean put(PutAction putAction, HTableInterface table) {
    for (PutActionModifier putActionModifier : putActionModifiers) {
      putAction = putActionModifier.modifyPutAction(putAction);
    }
    Put put = putAction.getPut();
    if (putAction.getVersionCheckAction() != null) {
      byte[] versionBytes = null;
      long version = putAction.getVersionCheckAction().getVersion();
      if (version != 0) {
        versionBytes = Bytes.toBytes(version);
      }
      try {
        return table.checkAndPut(put.getRow(), Constants.SYS_COL_FAMILY,
            Constants.VERSION_CHECK_COL_QUALIFIER, versionBytes, put);
      } catch (IOException e) {
        throw new DatasetIOException(
            "Error putting row from table with checkAndPut", e);
      }
    } else {
      try {
        table.put(put);
        return true;
      } catch (IOException e) {
        throw new DatasetIOException("Error putting row from table", e);
      }
    }
  }

  /**
   * Execute the put on HBase, invoking the putModifier before executing the put
   * if putModifier is not null.
   * 
   * Any PutModifers registered with registerPutModifier will be invoked after
   * the putModifier passed to this method is invoked, and before the Put is
   * executed.
   * 
   * @param putAction
   *          The PutAction to execute on HBase.
   * @param putActionModifier
   *          Invoked before the Put to give callers a chance to modify the Put
   *          before it is executed.
   * @return True if the put succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean put(PutAction putAction, PutActionModifier putActionModifier) {
    if (putActionModifier != null) {
      putAction = putActionModifier.modifyPutAction(putAction);
    }
    return put(putAction);
  }

  /**
   * Execute a Put on HBase, creating the Put by mapping the key and entity to a
   * Put with the entityMapper.
   * 
   * Any PutModifers registered with registerPutModifier will be invoked before
   * the Put is executed.
   * 
   * @param entity
   *          The entity to map to a Put with the entityMapper.
   * @param entityMapper
   *          The EntityMapper to map the key and entity to a put.
   * @return True if the put succeeded, False if the put failed due to update
   *         conflict
   */
  public <E> boolean put(E entity, EntityMapper<E> entityMapper) {
    return put(entity, null, entityMapper);
  }

  /**
   * Execute a Put on HBase, creating the Put by mapping the key and entity to a
   * Put with the entityMapper. putModifier will be invoked on this created Put
   * before the Put is executed.
   * 
   * Any PutModifers registered with registerPutModifier will be invoked after
   * the putModifier passed to this method is invoked, and before the Put is
   * executed.
   * 
   * @param entity
   *          The entity to map to a Put with the entityMapper.
   * @param putActionModifier
   *          Invoked before the Put to give callers a chance to modify the Put
   *          before it is executed.
   * @param entityMapper
   *          The EntityMapper to map the key and entity to a put.
   * @return True if the put succeeded, False if the put failed due to update
   *         conflict
   */
  public <E> boolean put(E entity, PutActionModifier putActionModifier,
      EntityMapper<E> entityMapper) {
    PutAction putAction = entityMapper.mapFromEntity(entity);
    return put(putAction, putActionModifier);
  }

  /**
   * Execute an increment on an entity field. This field must be a type that
   * supports increments. Returns the new increment value of type long.
   * 
   * @param key
   *          The key to map to an Increment
   * @param fieldName
   *          The name of the field we are incrementing
   * @param amount
   *          The amount to increment by
   * @param entityMapper
   *          The EntityMapper to map the key and increment amount to an
   *          Increment.
   * @return The new field amount after the increment.
   */
  public <E> long increment(PartitionKey key, String fieldName, long amount,
      EntityMapper<E> entityMapper) {
    Increment increment = entityMapper.mapToIncrement(key, fieldName, amount);
    HTableInterface table = pool.getTable(tableName);
    Result result;
    try {
      result = table.increment(increment);
    } catch (IOException e) {
      throw new DatasetIOException("Error incrementing field.", e);
    }
    return entityMapper.mapFromIncrementResult(result, fieldName);

  }

  /**
   * Execute a Delete on HBase.
   * 
   * Any DeleteActionModifers registered with registerDeleteModifier will be
   * invoked before the Delete is executed.
   * 
   * @param deleteAction
   *          The delete to execute on HBase.
   * @return True if the delete succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean delete(DeleteAction deleteAction) {
    HTableInterface table = pool.getTable(tableName);
    try {
      for (DeleteActionModifier deleteActionModifier : deleteActionModifiers) {
        deleteAction = deleteActionModifier.modifyDeleteAction(deleteAction);
      }
      Delete delete = deleteAction.getDelete();
      if (deleteAction.getVersionCheckAction() != null) {
        byte[] versionBytes = Bytes.toBytes(deleteAction
            .getVersionCheckAction().getVersion());
        try {
          return table.checkAndDelete(delete.getRow(),
              Constants.SYS_COL_FAMILY, Constants.VERSION_CHECK_COL_QUALIFIER,
              versionBytes, delete);
        } catch (IOException e) {
          throw new DatasetIOException(
              "Error deleteing row from table with checkAndDelete", e);
        }
      } else {
        try {
          table.delete(delete);
          return true;
        } catch (IOException e) {
          throw new DatasetIOException("Error deleteing row from table", e);
        }
      }
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          throw new DatasetIOException("Error putting table back into pool", e);
        }
      }
    }
  }

  /**
   * Execute the delete on HBase, invoking the deleteModifier before executing
   * the delete if deleteModifier is not null.
   * 
   * Any DeleteActionModifers registered with registerDeleteModifier will be
   * invoked after the deleteActionModifier passed to this method is invoked,
   * and before the Delete is executed.
   * 
   * @param deleteAction
   *          The Delete to execute.
   * @param deleteActionModifier
   *          Invoked before the Delete to give callers a chance to modify the
   *          Delete before it is executed.
   * @return True if the delete succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean delete(DeleteAction deleteAction,
      DeleteActionModifier deleteActionModifier) {
    if (deleteActionModifier != null) {
      deleteAction = deleteActionModifier.modifyDeleteAction(deleteAction);
    }
    return delete(deleteAction);
  }

  /**
   * Execute a Delete on HBase, creating the Delete from the key, and the set of
   * columns. Only the columns specified in this set will be deleted in the row.
   * 
   * Any DeleteActionModifers registered with registerDeleteModifier will be
   * invoked before the Delete is executed.
   * 
   * @param key
   *          The StorageKey to map to a Put with the entityMapper.
   * @param columns
   *          The set of columns to delete from the row.
   * @param checkAction
   *          A VersionCheckAction that will force this delete to do a
   *          checkAndDelete against a version count in the row
   * @return True if the delete succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean delete(PartitionKey key, Set<String> columns,
      VersionCheckAction checkAction, KeySerDe keySerDe) {
    return delete(key, columns, checkAction, null, keySerDe);
  }

  /**
   * Execute a Delete on HBase, creating the Delete from the key, and the set of
   * columns. Only the columns specified in this set will be deleted in the row.
   * deleteModifier will be invoked on this created Delete before the Delete is
   * executed.
   * 
   * Any DeleteActionModifers registered with registerDeleteActionModifier will
   * be invoked after the deleteActionModifier passed to this method is invoked,
   * and before the Delete is executed.
   * 
   * @param key
   *          The StorageKey to map to a Put with the entityMapper.
   * @param columns
   *          The set of columns to delete from the row.
   * @param checkAction
   *          A VersionCheckAction that will force this delete to do a
   *          checkAndDelete against a version count in the row
   * @param deleteActionModifier
   *          Invoked before the Delete to give callers a chance to modify the
   *          Delete before it is executed.
   * @return True if the delete succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean delete(PartitionKey key, Set<String> columns,
      VersionCheckAction checkAction,
      DeleteActionModifier deleteActionModifier, KeySerDe keySerDe) {
    byte[] keyBytes = keySerDe.serialize(key);
    Delete delete = new Delete(keyBytes);
    for (String requiredColumn : columns) {
      String[] familyAndColumn = requiredColumn.split(":");
      if (familyAndColumn.length == 1) {
        delete.deleteFamily(Bytes.toBytes(familyAndColumn[0]));
      } else {
        delete.deleteColumns(Bytes.toBytes(familyAndColumn[0]),
            Bytes.toBytes(familyAndColumn[1]));
      }
    }
    return delete(new DeleteAction(delete, checkAction), deleteActionModifier);
  }

  /**
   * Get an EntityScannerBuilder that the client can use to build an
   * EntityScanner.
   * 
   * @param entityMapper
   *          The EntityMapper to use to map rows to entities.
   * @return The EntityScannerBuilder
   */
  public <E> EntityScannerBuilder<E> getScannerBuilder(
      EntityMapper<E> entityMapper) {
    EntityScannerBuilder<E> builder = new BaseEntityScanner.Builder<E>(pool,
        tableName, entityMapper);
    for (ScanModifier scanModifier : scanModifiers) {
      builder.addScanModifier(scanModifier);
    }
    return builder;
  }

  /**
   * Create an EntityBatch that can be used to write batches of entities.
   *
   * @param entityMapper
   *          The EntityMapper to use to map rows to entities.
   * @param writeBufferSize
   *          The buffer size used when writing batches
   * @return EntityBatch
   */
  public <E> EntityBatch<E> createBatch(EntityMapper<E> entityMapper,
      long writeBufferSize) {
    return new BaseEntityBatch<E>(this, entityMapper, pool, tableName,
        writeBufferSize);
  }

  /**
   * Create an EntityBatch that can be used to write batches of entities.
   *
   * @param entityMapper
   *          The EntityMapper to use to map rows to entities.
   * @return EntityBatch
   */
  public <E> EntityBatch<E> createBatch(EntityMapper<E> entityMapper) {
    return new BaseEntityBatch<E>(this, entityMapper, pool, tableName);
  }
}
