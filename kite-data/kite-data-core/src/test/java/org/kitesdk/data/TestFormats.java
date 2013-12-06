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

import org.junit.Test;

import static org.junit.Assert.assertSame;

public class TestFormats {

  @Test
  public void testFromStringForValidFormats() {
    assertSame(Formats.AVRO, Formats.fromString("avro"));
    assertSame(Formats.PARQUET, Formats.fromString("parquet"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFromStringForInvalidFormats() {
    Formats.fromString("bogus");
  }

}
