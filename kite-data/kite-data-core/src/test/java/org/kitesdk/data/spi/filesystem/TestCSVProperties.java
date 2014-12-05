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

package org.kitesdk.data.spi.filesystem;

import org.junit.Assert;
import org.junit.Test;

public class TestCSVProperties {
  @Test
  public void testCSVProperitesBuilderDelimiter() {
    Assert.assertEquals("Delimiter should be tab",
        "\t",
        new CSVProperties.Builder()
            .delimiter("\\u0009")
            .build().delimiter);
    Assert.assertEquals("Delimiter should be tab",
        "\t",
        new CSVProperties.Builder()
            .delimiter("\\t")
            .build().delimiter);
    Assert.assertEquals("Delimiter should be tab",
        "\t",
        new CSVProperties.Builder()
            .delimiter("\t")
            .build().delimiter);
  }

  @Test
  public void testCSVProperitesBuilderEscape() {
    Assert.assertEquals("Escape should be backslash",
        "\\",
        new CSVProperties.Builder()
            .escape("\\u005c")
            .build().escape);
    Assert.assertEquals("Escape should be backslash",
        "\\",
        new CSVProperties.Builder()
            .escape("\\\\")
            .build().escape);
    Assert.assertEquals("Escape should be backslash",
        "\\",
        new CSVProperties.Builder()
            .escape("\\")
            .build().escape);
  }

  @Test
  public void testCSVProperitesBuilderQuote() {
    Assert.assertEquals("Quote should be '",
        "'",
        new CSVProperties.Builder()
            .quote("\\u0027")
            .build().quote);
    Assert.assertEquals("Quote should be '",
        "'",
        new CSVProperties.Builder()
            .quote("\\'")
            .build().quote);
    Assert.assertEquals("Quote should be '",
        "'",
        new CSVProperties.Builder()
            .quote("\'")
            .build().quote);
    Assert.assertEquals("Quote should be '",
        "'",
        new CSVProperties.Builder()
            .quote("'")
            .build().quote);
  }
}
