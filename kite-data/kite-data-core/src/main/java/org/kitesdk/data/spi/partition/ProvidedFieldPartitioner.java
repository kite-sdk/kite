/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi.partition;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.FieldPartitioner;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization.")
@Immutable
public class ProvidedFieldPartitioner<T extends Comparable> extends FieldPartitioner<T, T> {

  private static final String INT_TYPE = "int";
  private static final String LONG_TYPE = "long";
  private static final String STRING_TYPE = "string";

  public ProvidedFieldPartitioner(String name, Class<T> typeClass) {
    super(null, name, typeClass, typeClass, UNKNOWN_CARDINALITY);
  }

  @Override
  public T apply(T value) {
    return value;
  }

  @Override
  public Predicate<T> project(Predicate<T> predicate) {
    // uses no source data so there are no predicate projections
    return null;
  }

  @Override
  public Predicate<T> projectStrict(Predicate<T> predicate) {
    // uses no source data so there are no predicate projections
    return null;
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
      justification="Default annotation is not correct for equals")
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    ProvidedFieldPartitioner that = (ProvidedFieldPartitioner) o;
    return (Objects.equal(this.getName(), that.getName()) &&
            Objects.equal(this.getType(), that.getType()));
  }

  public String getTypeAsString() {
    return valuesString(getType());
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compare(T o1, T o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("name", getName())
        .add("values", getTypeAsString())
        .toString();
  }

  public static Class<? extends Comparable> valuesType(@Nullable String type) {
    if (type == null) {
      return String.class;
    } else if (STRING_TYPE.equals(type)) {
      return String.class;
    } else if (INT_TYPE.equals(type)) {
      return Integer.class;
    } else if (LONG_TYPE.equals(type)) {
      return Long.class;
    }
    throw new ValidationException("Not a valid provided type: " + type);
  }

  public static String valuesString(Class<? extends Comparable> type) {
    if (String.class.isAssignableFrom(type)) {
      return STRING_TYPE;
    } else if (Integer.class.isAssignableFrom(type)) {
      return INT_TYPE;
    } else if (Long.class.isAssignableFrom(type)) {
      return LONG_TYPE;
    }
    throw new ValidationException("Not a valid provided type: " + type);
  }
}
