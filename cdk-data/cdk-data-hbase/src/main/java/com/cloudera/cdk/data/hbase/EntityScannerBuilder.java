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
package com.cloudera.cdk.data.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.dao.EntityScanner;
import com.cloudera.cdk.data.hbase.filters.EntityFilter;
import com.cloudera.cdk.data.hbase.filters.RegexEntityFilter;
import com.cloudera.cdk.data.hbase.filters.SingleFieldEntityFilter;

/**
 * An abstract class that EntityScanner implementations should extend to offer a
 * builder interface. Implementations only need to implement the build method.
 * 
 * @param <K>
 *          The underlying key record type
 * @param <E>
 *          The entity type this canner scans
 */
public abstract class EntityScannerBuilder<E> {

  private HTablePool tablePool;
  private String tableName;
  private PartitionKey startKey;
  private PartitionKey stopKey;
  private int caching;
  private EntityMapper<E> entityMapper;
  private List<ScanModifier> scanModifiers = new ArrayList<ScanModifier>();
  private boolean passAllFilters = true;
  private List<Filter> filterList = new ArrayList<Filter>();

  /**
   * This is an abstract Builder object for the Entity Scanners, which will
   * allow users to dynamically construct a scanner object using the Builder
   * pattern. This is useful when the user doesn't have all the up front
   * information to create a scanner. It's also easier to add more options later
   * to the scanner, this will be the preferred method for users to create
   * scanners.
   * 
   * @param <K>
   *          The underlying key record type.
   * @param <E>
   *          The entity type this scanner scans.
   */
  public EntityScannerBuilder(HTablePool tablePool, String tableName, EntityMapper<E> entityMapper) {
    this.tablePool = tablePool;
    this.tableName = tableName;
    this.entityMapper = entityMapper;
  }

  /**
   * Get The HBase Table Pool
   * 
   * @return String
   */
  HTablePool getTablePool() {
    return tablePool;
  }

  /**
   * Get The HBase Table Name
   * 
   * @return String
   */
  String getTableName() {
    return tableName;
  }

  /**
   * Get The Entity Mapper
   * 
   * @return EntityMapper
   */
  EntityMapper<E> getEntityMapper() {
    return entityMapper;
  }

  /**
   * Get The Start Key
   * 
   * @return Key
   */
  PartitionKey getStartKey() {
    return startKey;
  }

  /**
   * Set The Start Key. If the partialStartKey has already been set, it will be
   * reset to null.
   * 
   * @param startKey
   *          The start key for this scan
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<E> setStartKey(PartitionKey startKey) {
    this.startKey = startKey;
    return this;
  }

  /**
   * Get The Start Key
   * 
   * @return Key
   */
  PartitionKey getStopKey() {
    return stopKey;
  }

  /**
   * Set The Stop Key. If a partialStopKey has already been set, it will be
   * reset to null.
   * 
   * @param stopKey
   *          The stop key for this scan
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<E> setStopKey(PartitionKey stopKey) {
    this.stopKey = stopKey;
    return this;
  }

  /**
   * Get the scanner caching value
   * 
   * @return the caching value
   */
  int getCaching() {
    return caching;
  }

  /**
   * Set The Scanner Caching Level
   * 
   * @param caching
   *          caching amount for scanner
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<E> setCaching(int caching) {
    this.caching = caching;
    return this;
  }

  /**
   * Add an Equality Filter to the Scanner, Will Filter Results Not Equal to the
   * Filter Value
   * 
   * @param fieldName
   *          The name of the column you want to apply the filter on
   * @param filterValue
   *          The value for comparison
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<E> addEqualFilter(String fieldName,
      Object filterValue) {
    SingleFieldEntityFilter singleFieldEntityFilter = new SingleFieldEntityFilter(
        entityMapper.getEntitySchema(), entityMapper.getEntitySerDe(),
        fieldName, filterValue, CompareFilter.CompareOp.EQUAL);
    filterList.add(singleFieldEntityFilter.getFilter());
    return this;
  }

  /**
   * Add an Inequality Filter to the Scanner, Will Filter Results Not Equal to
   * the Filter Value
   * 
   * @param fieldName
   *          The name of the column you want to apply the filter on
   * @param filterValue
   *          The value for comparison
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<E> addNotEqualFilter(String fieldName,
      Object filterValue) {
    SingleFieldEntityFilter singleFieldEntityFilter = new SingleFieldEntityFilter(
        entityMapper.getEntitySchema(), entityMapper.getEntitySerDe(),
        fieldName, filterValue, CompareFilter.CompareOp.NOT_EQUAL);
    filterList.add(singleFieldEntityFilter.getFilter());
    return this;
  }

  /**
   * Add a Regex Equality Filter to the Scanner, Will Filter Results Not Equal
   * to the Filter Value
   * 
   * @param fieldName
   *          The name of the column you want to apply the filter on
   * @param filterValue
   *          The regular expression to use for comparison
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<E> addRegexMatchFilter(String fieldName,
      String regexString) {
    RegexEntityFilter regexEntityFilter = new RegexEntityFilter(
        entityMapper.getEntitySchema(), entityMapper.getEntitySerDe(),
        fieldName, regexString);
    filterList.add(regexEntityFilter.getFilter());
    return this;
  }

  /**
   * Add a Regex Inequality Filter to the Scanner, Will Filter Results Not Equal
   * to the Filter Value
   * 
   * @param fieldName
   *          The name of the column you want to apply the filter on
   * @param filterValue
   *          The regular expression to use for comparison
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<E> addRegexNotMatchFilter(String fieldName,
      String regexString) {
    RegexEntityFilter regexEntityFilter = new RegexEntityFilter(
        entityMapper.getEntitySchema(), entityMapper.getEntitySerDe(),
        fieldName, regexString, false);
    filterList.add(regexEntityFilter.getFilter());
    return this;
  }

  /**
   * Do not include rows which have no value for fieldName
   * 
   * @param fieldName
   *          The field to check nullity
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<E> addNotNullFilter(String fieldName) {
    RegexEntityFilter regexEntityFilter = new RegexEntityFilter(
        entityMapper.getEntitySchema(), entityMapper.getEntitySerDe(),
        fieldName, ".+");
    filterList.add(regexEntityFilter.getFilter());
    return this;
  }

  /**
   * Only include rows which have an empty value for this field
   * 
   * @param fieldName
   *          The field to check nullity
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<E> addIsNullFilter(String fieldName) {
    RegexEntityFilter regexEntityFilter = new RegexEntityFilter(
        entityMapper.getEntitySchema(), entityMapper.getEntitySerDe(),
        fieldName, ".+", false);
    filterList.add(regexEntityFilter.getFilter());
    return this;
  }

  /**
   * Only include rows which are missing this field, this was the only possible
   * way to do it.
   * 
   * @param fieldName
   *          The field which should be missing
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<E> addIsMissingFilter(String fieldName) {
    SingleFieldEntityFilter singleFieldEntityFilter = new SingleFieldEntityFilter(
        entityMapper.getEntitySchema(), entityMapper.getEntitySerDe(),
        fieldName, "++++NON_SHALL_PASS++++", CompareFilter.CompareOp.EQUAL);
    SingleColumnValueFilter filter = (SingleColumnValueFilter) singleFieldEntityFilter
        .getFilter();
    filter.setFilterIfMissing(false);
    filterList.add(filter);
    return this;
  }

  /**
   * Check If All Filters Must Pass
   * 
   * @return boolean
   */
  boolean getPassAllFilters() {
    return passAllFilters;
  }

  public EntityScannerBuilder<E> setPassAllFilters(boolean passAllFilters) {
    this.passAllFilters = passAllFilters;
    return this;
  }

  /**
   * Get The List Of Filters For This Scanner
   * 
   * @return List
   */
  List<Filter> getFilterList() {
    return filterList;
  }

  /**
   * Add HBase Filter To Scan Object
   * 
   * @param filter
   *          EntityFilter object created by user
   */
  public EntityScannerBuilder<E> addFilter(EntityFilter filter) {
    filterList.add(filter.getFilter());
    return this;
  }

  /**
   * Get the ScanModifiers
   * 
   * @return List of ScanModifiers
   */
  List<ScanModifier> getScanModifiers() {
    return scanModifiers;
  }

  /**
   * Add the ScanModifier to the list of ScanModifiers.
   * 
   * @param scanModifier
   *          The ScanModifier to add
   */
  public EntityScannerBuilder<E> addScanModifier(ScanModifier scanModifier) {
    scanModifiers.add(scanModifier);
    return this;
  }

  /**
   * Build and return a EntityScanner object using all of the information
   * the user provided.
   * 
   * @return ScannerBuilder
   */
  public abstract EntityScanner<E> build();
}
