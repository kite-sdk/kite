// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.filters;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import com.cloudera.cdk.data.hbase.Dao;
import com.cloudera.cdk.data.hbase.EntitySchema;
import com.cloudera.cdk.data.hbase.EntitySchema.FieldMapping;
import com.cloudera.cdk.data.hbase.EntitySerDe;
import com.cloudera.cdk.data.hbase.HBaseCommonException;
import com.cloudera.cdk.data.hbase.MappingType;

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
    FieldMapping fieldMapping = entitySchema.getFieldMapping(fieldName);
    if (fieldMapping.getMappingType() != MappingType.COLUMN) {
      throw new HBaseCommonException(
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
