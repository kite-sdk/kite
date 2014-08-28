/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;

public class TestNestedFieldPartitioning {

  private static final Schema schema = SchemaBuilder.record("Station").fields()
      .requiredString("name")
      .name("position").type().record("Position").fields()
          .requiredDouble("latitude")
          .requiredDouble("longitude")
          .endRecord().noDefault()
      .endRecord();

  private static final PartitionStrategy strategy =
      new PartitionStrategy.Builder()
          .identity("position.latitude", "lat")
          .identity("position.longitude", "long")
          .build();

  private static class Station {
    private static class Position {
      private double latitude;
      private double longitude;
    }

    private Position position;
    private String name;

    private Station(String name, double lat, double lon) {
      this.name = name;
      this.position = new Position();
      this.position.latitude = lat;
      this.position.longitude = lon;
    }
  }

  @Test
  public void testDescriptorValidationPasses() {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .partitionStrategy(strategy)
        .build();
    Assert.assertEquals("Descriptor should have correct schema",
        schema, descriptor.getSchema());
    Assert.assertEquals("Descriptor should have correct strategy",
        strategy, descriptor.getPartitionStrategy());
  }

  @Test
  public void testDescriptorValidationFails() {
    final PartitionStrategy missingStartField =
        new PartitionStrategy.Builder()
            .identity("p.latitude")
            .identity("p.longitude")
            .build();

    TestHelpers.assertThrows("Should complain that p is missing",
        IllegalStateException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(schema)
                .partitionStrategy(missingStartField)
                .build();
          }
        });

    final PartitionStrategy missingLongField =
        new PartitionStrategy.Builder()
            .identity("position.latitude")
            .identity("position.long")
            .build();

    TestHelpers.assertThrows("Should complain that position.long is missing",
        IllegalStateException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(schema)
                .partitionStrategy(missingLongField)
                .build();
          }
        });

    final PartitionStrategy badExpectedType =
        new PartitionStrategy.Builder()
            .identity("position.latitude")
            .year("position.longitude")
            .build();

    TestHelpers.assertThrows("Should complain about mismatched expected type",
        IllegalStateException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(schema)
                .partitionStrategy(badExpectedType)
                .build();
          }
        });
  }

  @Test
  public void testStorageKeyForGenericEntity() {
    GenericData.Record position = new GenericData.Record(
        schema.getField("position").schema());
    position.put("latitude", 37.776);
    position.put("longitude", -122.418);
    GenericData.Record generic = new GenericData.Record(schema);
    generic.put("name", "KCASANFR291");
    generic.put("position", position);

    StorageKey key = new StorageKey.Builder(strategy)
        .add("position.latitude", 0)
        .add("position.longitude", 0)
        .build();

    StorageKey expected = StorageKey.copy(key);
    expected.replace(0, 37.776);
    expected.replace(1, -122.418);

    key.reuseFor(generic, DataModelUtil.accessor(GenericRecord.class, schema));

    Assert.assertEquals(expected, key);
  }

  @Test
  public void testStorageKeyForReflectedEntity() {
    Station station = new Station("KCASANFR291", 37.776, -122.418);

    StorageKey key = new StorageKey.Builder(strategy)
        .add("position.latitude", 0)
        .add("position.longitude", 0)
        .build();

    StorageKey expected = StorageKey.copy(key);
    expected.replace(0, 37.776);
    expected.replace(1, -122.418);

    key.reuseFor(station, DataModelUtil.accessor(Station.class, schema));

    Assert.assertEquals(expected, key);
  }
}
