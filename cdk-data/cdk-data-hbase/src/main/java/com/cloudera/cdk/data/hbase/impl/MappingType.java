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
package com.cloudera.cdk.data.hbase.impl;

/**
 * The supported Mapping Types, which control how an entity field maps to
 * columns in an HBase table.
 */
public enum MappingType {

  // Maps a value to a part of the row key
  KEY,
  
  // Maps a value to a single column.
  COLUMN,

  // Maps a map or record value to columns
  // in a column family.
  KEY_AS_COLUMN,
  
  // Maps a field to one that can be incremented
  COUNTER,
  
  // The field will be populated with the
  // current version of the entity. This
  // allows the version to be checked if this
  // same entity is persisted back, to make sure
  // it hasn't changed.
  OCC_VERSION
}
