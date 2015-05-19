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

public class TestFixedLongRangeFieldPartitioner {

  @Test(expected = IllegalArgumentException.class)
  public void testRangeNotPositive() {
    new FixedLongRangeFieldPartitioner("position", 0);
  }

  @Test
  public void test() {
    FixedLongRangeFieldPartitioner partitioner =
        new FixedLongRangeFieldPartitioner("position", 10);

    Assert.assertEquals(0, partitioner.apply(0L).longValue());
    Assert.assertEquals(0, partitioner.apply(9L).longValue());
    Assert.assertEquals(10, partitioner.apply(10L).longValue());
    Assert.assertEquals(10, partitioner.apply(11L).longValue());
    Assert.assertEquals(-10, partitioner.apply(-1L).longValue());
    Assert.assertEquals(-10, partitioner.apply(-9L).longValue());
    Assert.assertEquals(-10, partitioner.apply(-10L).longValue());
    Assert.assertEquals(-20, partitioner.apply(-11L).longValue());
  }

}
