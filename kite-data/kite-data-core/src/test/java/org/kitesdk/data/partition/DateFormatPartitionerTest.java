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

package org.kitesdk.data.partition;

import org.junit.Assert;
import org.junit.Test;

import java.util.TimeZone;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.impl.Accessor;

public class DateFormatPartitionerTest {
  @Test
  public void testApply() {
    long time = 1384912178434l;
    DateFormatPartitioner yyyyMMdd_UTC = new DateFormatPartitioner(
        "sourceField", "day", "yyyy-MM-dd");
    // Nov 19 ~5:50 PM PDT, but Nov 20 UTC
    Assert.assertEquals("2013-11-20", yyyyMMdd_UTC.apply(time));

    DateFormatPartitioner yyyyMMdd_PDT = new DateFormatPartitioner(
        "sourceField", "day", "yyyy-MM-dd", 1095, TimeZone.getTimeZone("PDT"));
    // same result because timestamps are _always_ UTC
    Assert.assertEquals("2013-11-20", yyyyMMdd_PDT.apply(time));
  }

  @Test
  public void testExpressionRoundTrip() {
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .dateFormat("timestamp", "day", "yyyy-MM-dd")
        .build();
    PartitionStrategy copy = Accessor.getDefault().fromExpression(
        Accessor.getDefault().toExpression(strategy));
    Assert.assertEquals(strategy, copy);
  }
}
