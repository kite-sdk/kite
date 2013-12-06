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

import static org.junit.Assert.assertEquals;

import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.PartitionStrategy;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger logger = LoggerFactory
      .getLogger(TestPartitionStrategy.class);

  @Test
  public void test() throws Exception {
    final PartitionStrategy p = new PartitionStrategy.Builder()
        .identity("month", Integer.class, 12)
        .hash("userId", 7)
        .build();

    List<FieldPartitioner> fieldPartitioners = p.getFieldPartitioners();
    Assert.assertEquals(2, fieldPartitioners.size());

    FieldPartitioner fp0 = fieldPartitioners.get(0);
    assertEquals("month", fp0.getName());
    assertEquals(12, fp0.getCardinality());

    FieldPartitioner fp1 = fieldPartitioners.get(1);
    assertEquals("userId", fp1.getName());
    assertEquals(7, fp1.getCardinality());

    assertEquals(12 * 7, p.getCardinality()); // useful for writers
  }

}
