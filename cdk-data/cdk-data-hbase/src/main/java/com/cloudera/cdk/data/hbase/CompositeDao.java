// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.dao.Dao;
import com.cloudera.cdk.data.dao.KeyEntity;
import java.util.List;

/**
 * The CompositeDao provides an interface for fetching from tables that may have
 * multiple entities stored per row.
 * 
 * The concept is that one entity can be composed from a list of KeyEntities
 * that make up the row, and that one entity can be decomposed into multiple sub
 * entities that can be persisted to the row.
 * 
 * @param <K>
 *          The type of the key
 * @param <E>
 *          The type of the entity this dao returns. This entity will be a
 *          composition of the sub entities.
 * @param <S>
 *          The type of the sub entities.
 */
public interface CompositeDao<K, E, S> extends Dao<K, E> {

  /**
   * Compose an entity from the list of sub-entities.
   * 
   * @param keyEntities The list of sub-entities
   * @return The KeyEntity instance which contains the composed entity.
   */
  public KeyEntity<K, E> compose(List<KeyEntity<K, S>> keyEntities);

  /**
   * Decompose an entity into multiple sub entities.
   * 
   * @param entity The entity to decompose
   * @return The list of subentities.
   */
  public List<S> decompose(E entity);
}
