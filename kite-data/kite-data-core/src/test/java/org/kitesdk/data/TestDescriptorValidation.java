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

package org.kitesdk.data;

import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

public class TestDescriptorValidation {

  @Test
  public void testAllowedPartitionSchemaCombinations() {
    Assert.assertNotNull("Should return a valid DatasetDescriptor",
        new DatasetDescriptor.Builder()
            .schema(SchemaBuilder.record("Record").fields()
                .requiredString("message")
                .requiredBoolean("bool")
                .requiredLong("timestamp")
                .requiredInt("number")
                .requiredDouble("double")
                .requiredFloat("float")
                .requiredBytes("payload")
                .endRecord())
            .partitionStrategy(new PartitionStrategy.Builder()
                .year("timestamp")
                .month("timestamp")
                .day("timestamp")
                .hour("timestamp")
                .minute("timestamp")
                .identity("message", "message_copy")
                .identity("timestamp", "ts")
                .identity("number", "num")
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
  public void testRejectsNullSchema() {
    TestHelpers.assertThrows("Should reject null schema",
        NullPointerException.class, new Runnable() {
      @Override
      public void run() {
        new DatasetDescriptor.Builder()
            .schema((Schema) null)
            .partitionStrategy(new PartitionStrategy.Builder()
                .hash("array", 48)
                .build())
            .build();
      }
    });
    TestHelpers.assertThrows("Should reject missing schema",
        ValidationException.class, new Runnable() {
      @Override
      public void run() {
        new DatasetDescriptor.Builder()
            .partitionStrategy(new PartitionStrategy.Builder()
                .hash("array", 48)
                .build())
            .build();
      }
    });
  }

  @Test
  public void testAllowsNonRecordSchemaWithoutPartitioning() {
    Assert.assertNotNull("Non-record should produce a valid descriptor",
        new DatasetDescriptor.Builder()
            .schema(Schema.createArray(Schema.create(Schema.Type.FLOAT)))
            .build());
  }

  @Test
  public void testRejectsPartitioningWithNonRecordSchema() {
    TestHelpers.assertThrows("Should reject partitioning without a record",
        ValidationException.class, new Runnable() {
      @Override
      public void run() {
        new DatasetDescriptor.Builder()
            .schema(Schema.createArray(Schema.create(Schema.Type.FLOAT)))
            .partitionStrategy(new PartitionStrategy.Builder()
                .hash("array", 48)
                .build())
            .build();
      }
    });
  }

  @Test
  public void testRejectsPartitioningOnMissingField() {
    TestHelpers.assertThrows("Should reject partitioning on a missing field",
        ValidationException.class, new Runnable() {
      @Override
      public void run() {
        new DatasetDescriptor.Builder()
            .schema(SchemaBuilder.record("Record").fields()
                .requiredString("field")
                .endRecord())
            .partitionStrategy(new PartitionStrategy.Builder()
                .hash("array", 48)
                .build())
            .build();
      }
    });
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testRejectsSchemaPartitionerTypeMismatch() {
    // obvious type mismatches
    TestHelpers.assertThrows("Should reject int for long partitioner",
        ValidationException.class, new Runnable() {
      @Override
      public void run() {
        new DatasetDescriptor.Builder()
            .schema(SchemaBuilder.record("Record").fields()
                .requiredInt("timestamp")
                .endRecord())
            .partitionStrategy(new PartitionStrategy.Builder()
                .year("timestamp").month("timestamp").day("timestamp")
                .build())
            .build();
      }
    });
    TestHelpers.assertThrows("Should reject string for int partitioner",
        ValidationException.class, new Runnable() {
      @Override
      public void run() {
        new DatasetDescriptor.Builder()
            .schema(SchemaBuilder.record("Record").fields()
                .requiredString("username")
                .endRecord())
            .partitionStrategy(new PartitionStrategy.Builder()
                .range("number", 5, 10, 15)
                .build())
            .build();
      }
    });
    TestHelpers.assertThrows("Should reject int for string partitioner",
        ValidationException.class, new Runnable() {
      @Override
      public void run() {
        new DatasetDescriptor.Builder()
            .schema(SchemaBuilder.record("Record").fields()
                .requiredInt("number")
                .endRecord())
            .partitionStrategy(new PartitionStrategy.Builder()
                .range("number", "m", "z")
                .build())
            .build();
      }
    });
    // Cannot assign to a Long from an Integer
    TestHelpers.assertThrows("Should reject int for long partitioner",
        ValidationException.class, new Runnable() {
      @Override
      public void run() {
        new DatasetDescriptor.Builder()
            .schema(SchemaBuilder.record("Record").fields()
                .requiredInt("timestamp")
                .endRecord())
            .partitionStrategy(new PartitionStrategy.Builder()
                .year("timestamp")
                .build())
            .build();
      }
    });
  }
}
