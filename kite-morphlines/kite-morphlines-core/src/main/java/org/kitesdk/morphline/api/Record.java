/*
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
package org.kitesdk.morphline.api;

import java.util.Collection;
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
  
  private ArrayListMultimap<String, Object> fields;

  /** Creates a new empty record. */
  public Record() {
    this(create());
  }
  
  private Record(ArrayListMultimap<String, Object> fields) {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  /** Returns a shallow copy of this record. */
  public Record copy() {
    //return new Record(ArrayListMultimap.create(fields)); // adding fields later causes (slow) rehashing
    ArrayListMultimap<String,Object> copy = ArrayListMultimap.create(fields.size() + 16, 10);
    copy.putAll(fields);
    return new Record(copy);
  }
  
  /** Returns the fields that are stored in this record. */
  public ListMultimap<String, Object> getFields() {
    return fields;
  }

  /**
   * Returns a view of the values associated with the given key. An empty collection may be
   * returned, but never <code>null</null>.
   */
  public List get(String key) {
    return fields.get(key);
  }
  
  /** Adds the given value to the values currently associated with the given key. */
  public void put(String key, Object value) {
    fields.put(key, value);    
  }
  
  /** Returns the first value associated with the given key, or null if no such value exists */
  public Object getFirstValue(String key) {
    List values = fields.get(key);
    return values.size() > 0 ? values.get(0) : null;
  }

  /**
   * Removes all values that are associated with the given key, and then associates the given value
   * with the given key.
   */
  public void replaceValues(String key, Object value) {
//    fields.replaceValues(key, Collections.singletonList(value)); // unnecessarily slow
    List<Object> list = fields.get(key);
    list.clear(); 
    list.add(value);
  }
  
  /** Removes all values that are associated with the given key */
  public void removeAll(String key) {
    //fields.removeAll(key); // unnecessarily slow
    fields.get(key).clear();
  }
  
  /**
   * Adds the given value to the values currently associated with the given key, iff the key isn't
   * already associated with that same value.
   */
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
    return new TreeMap<String, Collection<Object>>(fields.asMap()).toString();
  }

  private static ArrayListMultimap<String, Object> create() {
    return ArrayListMultimap.create();
  }
  
}
