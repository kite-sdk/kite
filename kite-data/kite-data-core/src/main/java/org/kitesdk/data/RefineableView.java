/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data;

import javax.annotation.concurrent.Immutable;

/**
 * A {@code RefineableView} specifies a subset of a {@link Dataset} by one or more
 * logical constraints.
 *
 * @param <E>
 *      The type of entities stored in the {@code Dataset} underlying this
 *      {@code RefineableView}.
 * @since 0.11.0
 */
@Immutable
public interface RefineableView<E> extends View<E> {

  /**
   * Creates a sub-{@code View}, restricted to entities whose <code>name</code> field is
   * equal to any of the given <code>values</code>. If no <code>values</code> are
   * specified, then the view is restricted to entities whose <code>name</code> field
   * is non-null.
   *
   * @param name the field name of the entity
   * @return the restricted view
   */
  RefineableView<E> with(String name, Object... values);

  /**
   * Creates a sub-{@code View}, restricted to entities whose <code>name</code> field
   * is greater than or equal to the given <code>value</code>.
   *
   * @param name the field name of the entity
   * @return the restricted view
   */
  RefineableView<E> from(String name, Comparable value);

  /**
   * Creates a sub-{@code View}, restricted to entities whose <code>name</code> field
   * is greater than to the given <code>value</code>.
   *
   * @param name the field name of the entity
   * @return the restricted view
   */
  RefineableView<E> fromAfter(String name, Comparable value);

  /**
   * Creates a sub-{@code View}, restricted to entities whose <code>name</code> field
   * is less than or equal to the given <code>value</code>.
   *
   * @param name the field name of the entity
   * @return the restricted view
   */
  RefineableView<E> to(String name, Comparable value);

  /**
   * Creates a sub-{@code View}, restricted to entities whose <code>name</code> field
   * is less than to the given <code>value</code>.
   *
   * @param name the field name of the entity
   * @return the restricted view
   */
  RefineableView<E> toBefore(String name, Comparable value);

}
