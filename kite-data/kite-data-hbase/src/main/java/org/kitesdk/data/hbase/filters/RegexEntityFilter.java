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
package org.kitesdk.data.hbase.filters;

import java.lang.reflect.Constructor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.FieldMapping.MappingType;
import org.kitesdk.data.hbase.impl.EntitySchema;
import org.kitesdk.data.hbase.impl.EntitySerDe;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * An EntityFilter that will perform a regular expression filter on an entity's
 * field.
 */
public class RegexEntityFilter implements EntityFilter {

  private final Filter filter;

  public RegexEntityFilter(EntitySchema entitySchema,
      EntitySerDe<?> entitySerDe, String fieldName, String regex,
      boolean isEqual) {
    FieldMapping fieldMapping = entitySchema.getColumnMappingDescriptor()
        .getFieldMapping(fieldName);
    if (fieldMapping.getMappingType() != MappingType.COLUMN) {
      throw new DatasetException(
          "SingleColumnValueFilter only compatible with COLUMN mapping types.");
    }

    this.filter = constructFilter(regex, isEqual, fieldMapping);
  }

  private Filter constructFilter(String regex, boolean isEqual, FieldMapping fieldMapping) {
    byte[] family = fieldMapping.getFamily();
    byte[] qualifier = fieldMapping.getQualifier();

    try {
      // To work we both HBase 0.94 and 0.96 we have to use reflection to construct a
      // SingleColumnValueFilter (and a RegexStringComparator) since
      // WritableByteArrayComparable (which RegexStringComparator extends) was renamed
      // to ByteArrayComparable in HBase 0.95 (HBASE-6658)
      Class<?> c = Class.forName("org.apache.hadoop.hbase.filter.SingleColumnValueFilter");
      for (Constructor<?> cons : c.getConstructors()) {
        if (cons.getParameterTypes().length == 4 &&
            !cons.getParameterTypes()[3].isArray()) { // not byte[] as the fourth arg
          Object regexStringComparator = Class.forName(
              "org.apache.hadoop.hbase.filter.RegexStringComparator")
              .getConstructor(String.class).newInstance(regex);
          return (Filter) cons.newInstance(family, qualifier,
              isEqual ? CompareFilter.CompareOp.EQUAL
              : CompareFilter.CompareOp.NOT_EQUAL, regexStringComparator);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new DatasetException("Cannot create RegexEntityFilter.", e);
    }
    throw new DatasetException("Cannot create RegexEntityFilter (no constructor found).");
  }

  public RegexEntityFilter(EntitySchema entitySchema,
      EntitySerDe<?> entitySerDe, String fieldName, String regex) {
    this(entitySchema, entitySerDe, fieldName, regex, true);
  }

  public Filter getFilter() {
    return filter;
  }
}
