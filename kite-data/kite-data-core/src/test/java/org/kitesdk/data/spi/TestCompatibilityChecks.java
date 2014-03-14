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
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;

public class TestCompatibilityChecks {

  private static final Schema schema = SchemaBuilder.record("Record").fields()
      .requiredString("message")
      .requiredBoolean("bool")
      .requiredLong("timestamp")
      .requiredInt("number")
      .requiredDouble("double")
      .requiredFloat("float")
      .requiredBytes("payload")
      .endRecord();

  @Test
  public void testAllowedPartitionSchemaCombinations() {
    Compatibility.checkDescriptor(
        new DatasetDescriptor.Builder()
            .schema(schema)
            .partitionStrategy(new PartitionStrategy.Builder()
                .year("timestamp")
                .month("timestamp")
                .day("timestamp")
                .hour("timestamp")
                .minute("timestamp")
                .identity("message", "message_copy", String.class, -1)
                .identity("timestamp", "ts", Long.class, -1)
                .identity("number", "num", Integer.class, -1)
                .hash("message", 48)
                .hash("timestamp", 48)
                .hash("number", 48)
                .hash("payload", 48)
                .hash("float", 48)
                .hash("double", 48)
                .hash("bool", 48)
                .range("number", 5, 10, 15, 20)
                .range("message", "m", "z", "M", "Z")
                .build())
            .build());
  }

  @Test
  public void testNullDescriptor() {
    TestHelpers.assertThrows("Should reject null descriptor",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        Compatibility.checkDescriptor(null);
      }
    });
  }

  @Test
  public void testIllegalPartitionNames() {
    // no need to check sources because '.' and '-' aren't allowed in schemas
    TestHelpers.assertThrows("Should reject '-' in partition name",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        Compatibility.checkDescriptor(
            new DatasetDescriptor.Builder()
                .schema(schema)
                .partitionStrategy(new PartitionStrategy.Builder()
                    .identity("day_of_month", "day-of-month", Integer.class, 31)
                    .build())
                .build());
      }
    });
    TestHelpers.assertThrows("Should reject '.' in partition name",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        Compatibility.checkDescriptor(
            new DatasetDescriptor.Builder()
                .schema(schema)
                .partitionStrategy(new PartitionStrategy.Builder()
                    .identity("number", "day.of.month", Integer.class, 31)
                    .build())
                .build());
      }
    });
  }

  @Test
  public void testDuplicatePartitionNames() {
    TestHelpers.assertThrows(
        "Should reject partition names that duplicate partition names",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        Compatibility.checkDescriptor(
            new DatasetDescriptor.Builder()
                .schema(schema)
                .partitionStrategy(new PartitionStrategy.Builder()
                    .day("timestamp")
                    .identity("number", "day", Integer.class, 31)
                    .build())
                .build());
      }
    });
    TestHelpers.assertThrows(
        "Should reject partition names that duplicate source names",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        Compatibility.checkDescriptor(
            new DatasetDescriptor.Builder()
                .schema(schema)
                .partitionStrategy(new PartitionStrategy.Builder()
                    .day("timestamp")
                    .identity("number", "timestamp", Integer.class, 31)
                    .build())
                .build());
      }
    });
  }

}
