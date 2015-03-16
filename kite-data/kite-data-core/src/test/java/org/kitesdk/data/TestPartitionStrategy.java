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
package org.kitesdk.data;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.data.spi.FieldPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class TestPartitionStrategy {

  static class Entity {
    int month;
    int userId;

    public int getMonth() {
      return month;
    }

    public void setMonth(int month) {
      this.month = month;
    }

    public int getUserId() {
      return userId;
    }

    public void setUserId(int userId) {
      this.userId = userId;
    }
  }

  private static final Logger LOG = LoggerFactory
      .getLogger(TestPartitionStrategy.class);

  @Test
  public void test() throws Exception {
    final PartitionStrategy p = new PartitionStrategy.Builder()
        .identity("month", "month_ordinal", 12)
        .hash("userId", 7)
        .build();

    List<FieldPartitioner> fieldPartitioners = p.getFieldPartitioners();
    Assert.assertEquals(2, fieldPartitioners.size());

    FieldPartitioner fp0 = fieldPartitioners.get(0);
    assertEquals("month_ordinal", fp0.getName());
    assertEquals(12, fp0.getCardinality());

    FieldPartitioner fp1 = fieldPartitioners.get(1);
    assertEquals("userId_hash", fp1.getName());
    assertEquals(7, fp1.getCardinality());

    assertEquals(12 * 7, p.getCardinality()); // useful for writers
  }

  @Test
  @Ignore
  public void testDuplicateFieldNames() {
    Assert.assertNotNull("Should allow duplicate source fields",
        new PartitionStrategy.Builder()
            .year("timestamp").month("timestamp")
            .build());
    TestHelpers.assertThrows("Should reject duplicate partition fields",
        ValidationException.class, new Runnable() {
      @Override
      public void run() {
        new PartitionStrategy.Builder()
            .identity("number", "num")
            .identity("number2", "num")
            .build();
      }
    });
  }

}
