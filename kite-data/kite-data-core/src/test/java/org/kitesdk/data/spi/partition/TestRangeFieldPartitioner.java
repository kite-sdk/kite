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
package org.kitesdk.data.spi.partition;

import org.junit.Assert;
import org.junit.Test;

public class TestRangeFieldPartitioner {

  @Test
  public void test() {
    RangeFieldPartitioner partitioner = new RangeFieldPartitioner("username",
        new String[] { "barry", "susan", "zippy" });

    Assert.assertEquals(3, partitioner.getCardinality());
    Assert.assertEquals("barry", partitioner.apply("adam"));
    Assert.assertEquals("barry", partitioner.apply("barry"));
    Assert.assertEquals("susan", partitioner.apply("charlie"));
    Assert.assertEquals("zippy", partitioner.apply("yvette"));

    Exception ex = null;

    try {
      partitioner.apply("zzzzz out of range");
    } catch (Exception e) {
      ex = e;
    }

    Assert.assertNotNull(ex);
    Assert.assertTrue(ex instanceof IllegalArgumentException);
  }

}
