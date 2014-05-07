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

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.FieldMapping.MappingType;
import org.kitesdk.data.hbase.impl.EntitySchema;
import org.kitesdk.data.hbase.impl.EntitySerDe;

/**
 * An EntityFilter that will perform an equality filter on a single entity
 * field.
 */
public class SingleFieldEntityFilter implements EntityFilter {

  private final org.apache.hadoop.hbase.filter.Filter filter;

  public SingleFieldEntityFilter(EntitySchema entitySchema,
      EntitySerDe<?> entitySerDe, String fieldName, Object filterValue) {
    this(entitySchema, entitySerDe, fieldName, filterValue,
        CompareFilter.CompareOp.EQUAL);
  }

  public SingleFieldEntityFilter(EntitySchema entitySchema,
      EntitySerDe<?> entitySerDe, String fieldName, Object filterValue,
      CompareFilter.CompareOp equalityOperator) {
    FieldMapping fieldMapping = entitySchema.getColumnMappingDescriptor()
        .getFieldMapping(fieldName);
    if (fieldMapping.getMappingType() != MappingType.COLUMN) {
      throw new DatasetException(
          "SingleColumnValueFilter only compatible with COLUMN mapping types.");
    }

    byte[] family = fieldMapping.getFamily();
    byte[] qualifier = fieldMapping.getQualifier();
    byte[] comparisonBytes = entitySerDe.serializeColumnValueToBytes(fieldName,
        filterValue);

    this.filter = new SingleColumnValueFilter(family, qualifier,
        equalityOperator, comparisonBytes);
  }

  public Filter getFilter() {
    return filter;
  }
}
