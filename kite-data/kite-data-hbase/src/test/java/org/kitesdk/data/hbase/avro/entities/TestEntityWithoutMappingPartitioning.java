/**
 * Copyright 2016 Cloudera Inc.
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
package org.kitesdk.data.hbase.avro.entities;

import org.apache.avro.Schema;

/**
 * Mimic a generated avro scema without mapping and partitioning information.
 */
public class TestEntityWithoutMappingPartitioning implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TestEntity\",\"namespace\":\"org.kitesdk.data.hbase.avro.entities\",\"fields\":[{\"name\":\"part1\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"part2\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"field1\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"field2\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"enum\",\"type\":{\"type\":\"enum\",\"name\":\"TestEnum\",\"symbols\":[\"ENUM1\",\"ENUM2\",\"ENUM3\"]}},{\"name\":\"field3\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"avro.java.string\":\"String\"}},{\"name\":\"field4\",\"type\":{\"type\":\"record\",\"name\":\"EmbeddedRecord\",\"fields\":[{\"name\":\"embeddedField1\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"embeddedField2\",\"type\":\"long\"}]}},{\"name\":\"field5\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ArrayRecord\",\"fields\":[{\"name\":\"subfield1\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"subfield2\",\"type\":\"long\"},{\"name\":\"subfield3\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}}},{\"name\":\"version\",\"type\":\"long\",\"default\":0}]}");

  private TestEntity delegate = new TestEntity();

  @Override
  public void put(int i, Object v) {
    delegate.put(i, v);
  }

  @Override
  public Object get(int i) {
    return delegate.get(i);
  }

  @Override
  public Schema getSchema() {
    return SCHEMA$;
  }
}
