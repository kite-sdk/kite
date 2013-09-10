// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.filters;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

import com.cloudera.cdk.data.hbase.Dao;
import com.cloudera.cdk.data.hbase.EntitySchema;
import com.cloudera.cdk.data.hbase.EntitySchema.FieldMapping;
import com.cloudera.cdk.data.hbase.EntitySerDe;
import com.cloudera.cdk.data.hbase.HBaseCommonException;
import com.cloudera.cdk.data.hbase.MappingType;

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
      throw new HBaseCommonException(
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
