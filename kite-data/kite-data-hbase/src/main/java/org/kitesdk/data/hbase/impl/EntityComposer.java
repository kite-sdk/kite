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

import java.util.Map;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.PartitionKey;

/**
 * An EntityComposer is an interface that supports entity construction and
 * de-construction methods. This includes getting a Builder for an entity,
 * building keyAsColumn field values, and extracting fields and keyAsColumn
 * fields.
 * 
 * These are basically all methods an EntityMapper would need to do the entity
 * record construction/de-construction from HBase column values.
 * 
 * @param <E>
 *          The type of entity this composer works with.
 */
public interface EntityComposer<E> {

  /**
   * Get an Entity Builder that can build Entity types for this composer.
   * 
   * @return The entity builder.
   */
  public Builder<E> getBuilder();

  /**
   * Extract a field from the entity by name
   * 
   * @param entity
   *          The entity to extract a field from.
   * @param fieldName
   *          The name of the field to extract
   * @return The field value
   */
  public Object extractField(E entity, String fieldName);

  /**
   * Extract a PartitionKey for the given Strategy.
   *
   * @param strategy a PartitionStrategy for the entity
   * @param entity The entity to extract a partition key from.
   * @return a PartitionKey for the entity
   */
  public PartitionKey extractKey(PartitionStrategy strategy, E entity);

  /**
   * Transform the keyAsColumn field value into a Map
   * 
   * @param fieldName
   *          The name of the keyAsColumn field.
   * @param fieldValue
   *          The value of the entities field specified by field name. The value
   *          can be any type the implementation supports for keyAsColumn fields
   * @return The keyAsColumn field value as a map
   */
  public Map<CharSequence, Object> extractKeyAsColumnValues(String fieldName,
      Object fieldValue);

  /**
   * Build a keyAsColumn field for the entity from a map of keyAsColumn values.
   * This is the inverse of extractKeyAsColumnValues. It will turn the map into
   * the type the entity uses for its keyAsColumn field.
   * 
   * @param fieldName
   *          The name of the field
   * @param keyAsColumnValues
   *          The map of keyAsColumn values.
   * @return The field value
   */
  public Object buildKeyAsColumnField(String fieldName,
      Map<CharSequence, Object> keyAsColumnValues);

  /**
   * An interface for entity builders.
   * 
   * @param <E>
   *          The type of the entity this builder builds.
   */
  public interface Builder<E> {

    /**
     * Put a field value into the entity.
     * 
     * @param fieldName
     *          The name of the field
     * @param value
     *          The value of the field
     * @return A reference to the Builder, so puts can be chained.
     */
    public Builder<E> put(String fieldName, Object value);

    /**
     * Builds the entity, and returns it.
     * 
     * @return The built entity
     */
    public E build();
  }
}
