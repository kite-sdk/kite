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
package com.cloudera.cdk.morphline.api;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * A record is a set of named fields where each field has a list of one or more values.
 * 
 * A value can be of any type, i.e. any Java Object. That is, a record is a {@link ListMultimap} as
 * in Guava’s {@link ArrayListMultimap}. Note that a field can be multi-valued and that any two
 * records need not use common field names. This flexible data model corresponds exactly to the
 * characteristics of the Solr/Lucene data model (i.e. a record is a SolrInputDocument). A field
 * with zero values is removed from the record - it does not exist as such.
 */
public final class Record {
  
  private ListMultimap<String, Object> fields;

  public Record() {
    this(create());
  }
  
  private Record(ListMultimap<String, Object> fields) {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  public Record copy() {
    return new Record(ArrayListMultimap.create(getFields()));
  }
  
  public ListMultimap<String, Object> getFields() {
    return fields;
  }

  public List get(String key) {
    return fields.get(key);
  }
  
  public void put(String key, Object value) {
    fields.put(key, value);    
  }
  
  public Object getFirstValue(String key) {
    List values = fields.get(key);
    return values.size() > 0 ? values.get(0) : null;
  }
  
  public void replaceValues(String key, Object value) {
    fields.replaceValues(key, Collections.singletonList(value)); // TODO optimize?
  }
  
  public void removeAll(String key) {
    fields.removeAll(key);
  }
  
  public void putIfAbsent(String key, Object value) {
    if (!fields.containsEntry(key, value)) {
      fields.put(key, value);
    }
  }
  
  @Override
  public boolean equals(Object other) {
    if (other instanceof Record) {
      return fields.equals(((Record)other).getFields());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return fields.hashCode();
  }
  
  @Override
  public String toString() { // print fields sorted by key for better human readability
    return new TreeMap(fields.asMap()).toString();
  }

  private static ListMultimap<String, Object> create() {
    return ArrayListMultimap.create();
  }
  
}
