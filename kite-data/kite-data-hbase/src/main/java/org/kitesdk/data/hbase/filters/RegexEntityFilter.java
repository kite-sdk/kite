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

import org.kitesdk.data.DatasetException;
import org.kitesdk.data.hbase.impl.EntitySchema;
import org.kitesdk.data.hbase.impl.EntitySchema.FieldMapping;
import org.kitesdk.data.hbase.impl.EntitySerDe;
import org.kitesdk.data.hbase.impl.MappingType;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

/**
 * An EntityFilter that will perform a regular expression filter on an entity's
 * field.
 */
public class RegexEntityFilter implements EntityFilter {

  private final Filter filter;

  public RegexEntityFilter(EntitySchema entitySchema,
      EntitySerDe<?> entitySerDe, String fieldName, String regex,
      boolean isEqual) {
    FieldMapping fieldMapping = entitySchema.getFieldMapping(fieldName);
    if (fieldMapping.getMappingType() != MappingType.COLUMN) {
      throw new DatasetException(
          "SingleColumnValueFilter only compatible with COLUMN mapping types.");
    }

    byte[] family = fieldMapping.getFamily();
    byte[] qualifier = fieldMapping.getQualifier();

    this.filter = new org.apache.hadoop.hbase.filter.SingleColumnValueFilter(
        family, qualifier, isEqual ? CompareFilter.CompareOp.EQUAL
            : CompareFilter.CompareOp.NOT_EQUAL, new RegexStringComparator(
            regex));
  }

  public RegexEntityFilter(EntitySchema entitySchema,
      EntitySerDe<?> entitySerDe, String fieldName, String regex) {
    this(entitySchema, entitySerDe, fieldName, regex, true);
  }

  public Filter getFilter() {
    return filter;
  }
}
