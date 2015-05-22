/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.spi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.ValidationException;

public class TestPartitionStrategyParser {

  public static void checkParser(PartitionStrategy expected, String json) {
    PartitionStrategy parsed = PartitionStrategyParser.parse(json);
    Assert.assertEquals(expected, parsed);

    parsed = PartitionStrategyParser.parse(expected.toString());
    Assert.assertEquals("Should reparse properly", expected, parsed);
  }

  @Test
  public void testIdentity() {
    // right now, the field type is taken from the Schema
    checkParser(new PartitionStrategy.Builder()
            .identity("username", "id")
            .build(),
        "[ {\"type\": \"identity\", " +
            "\"source\": \"username\", " +
            "\"name\": \"id\"} ]"
    );
    checkParser(new PartitionStrategy.Builder()
            .identity("username", "username_copy")
            .build(),
        "[ {\"type\": \"identity\", \"source\": \"username\"} ]"
    );
  }

  @Test
  public void testHash() {
    checkParser(new PartitionStrategy.Builder().hash("id", 64).build(),
        "[ {\"type\": \"hash\", \"source\": \"id\", \"buckets\": 64} ]");
    checkParser(new PartitionStrategy.Builder().hash("id", "h", 64).build(),
        "[ {\"type\": \"hash\", " +
            "\"source\": \"id\", " +
            "\"name\": \"h\", " +
            "\"buckets\": 64} ]"
    );

    TestHelpers.assertThrows("Should reject missing buckets",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {\"type\": \"hash\", " +
                "\"source\": \"id\", " +
                "\"name\": \"h\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject invalid buckets",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {\"type\": \"hash\", " +
                "\"source\": \"id\", " +
                "\"name\": \"h\", " +
                "\"buckets\": \"green\"} ]");
          }
        }
    );
  }

  @Test
  public void testFixedSizedRange() {
    checkParser(new PartitionStrategy.Builder().fixedSizeRange("id", 64).build(),
        "[ {\"type\": \"range\", \"source\": \"id\", \"size\": 64} ]");
    checkParser(new PartitionStrategy.Builder().fixedSizeRange("id", "rng", 64).build(),
        "[ {\"type\": \"range\", " +
            "\"source\": \"id\", " +
            "\"name\": \"rng\", " +
            "\"size\": 64} ]"
    );

    TestHelpers.assertThrows("Should reject missing size",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {\"type\": \"range\", " +
                "\"source\": \"id\", " +
                "\"name\": \"rng\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject invalid size",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {\"type\": \"range\", " +
                "\"source\": \"id\", " +
                "\"name\": \"rng\", " +
                "\"size\": \"green\"} ]");
          }
        }
    );
  }

  @Test
  public void testDateFormat() {
    checkParser(new PartitionStrategy.Builder()
            .dateFormat("time", "date", "yyyyMMdd")
            .build(),
        "[ {\"type\": \"dateFormat\", " +
            "\"source\": \"time\", " +
            "\"name\": \"date\", " +
            "\"format\": \"yyyyMMdd\"} ]");

    TestHelpers.assertThrows("Should reject missing format",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {\"type\": \"dateFormat\", " +
                "\"source\": \"time\", " +
                "\"name\": \"date\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject missing name",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {\"type\": \"dateFormat\", " +
                "\"source\": \"time\", " +
                "\"format\": \"yyyyMMdd\"} ]");
          }
        }
    );
  }

  @Test
  public void testYear() {
    checkParser(new PartitionStrategy.Builder().year("time").build(),
        "[ {\"type\": \"year\", \"source\": \"time\"} ]");
    checkParser(new PartitionStrategy.Builder().year("time", "y").build(),
        "[ {\"type\": \"year\", \"source\": \"time\", \"name\": \"y\"} ]");
  }

  @Test
  public void testMonth() {
    checkParser(new PartitionStrategy.Builder().month("time").build(),
        "[ {\"type\": \"month\", \"source\": \"time\"} ]");
    checkParser(new PartitionStrategy.Builder().month("time", "m").build(),
        "[ {\"type\": \"month\", \"source\": \"time\", \"name\": \"m\"} ]");
  }

  @Test
  public void testDay() {
    checkParser(new PartitionStrategy.Builder().day("time").build(),
        "[ {\"type\": \"day\", \"source\": \"time\"} ]");
    checkParser(new PartitionStrategy.Builder().day("time", "d").build(),
        "[ {\"type\": \"day\", \"source\": \"time\", \"name\": \"d\"} ]");
  }

  @Test
  public void testHour() {
    checkParser(new PartitionStrategy.Builder().hour("time").build(),
        "[ {\"type\": \"hour\", \"source\": \"time\"} ]");
    checkParser(new PartitionStrategy.Builder().hour("time", "h").build(),
        "[ {\"type\": \"hour\", \"source\": \"time\", \"name\": \"h\"} ]");
  }

  @Test
  public void testMinute() {
    checkParser(new PartitionStrategy.Builder().minute("time").build(),
        "[ {\"type\": \"minute\", \"source\": \"time\"} ]");
    checkParser(new PartitionStrategy.Builder().minute("time", "m").build(),
        "[ {\"type\": \"minute\", \"source\": \"time\", \"name\": \"m\"} ]");
  }

  @Test
  public void testProvided() {
    checkParser(new PartitionStrategy.Builder().provided("version").build(),
        "[ {\"type\": \"provided\", \"name\": \"version\"} ]");
    checkParser(new PartitionStrategy.Builder().provided("version").build(),
        "[ {\"type\": \"provided\", \"name\": \"version\", \"values\": \"string\"} ]");
    checkParser(new PartitionStrategy.Builder().provided("version", "string").build(),
        "[ {\"type\": \"provided\", \"name\": \"version\", \"values\": \"string\"} ]");
    checkParser(new PartitionStrategy.Builder().provided("version", "int").build(),
        "[ {\"type\": \"provided\", \"name\": \"version\", \"values\": \"int\"} ]");
    checkParser(new PartitionStrategy.Builder().provided("version", "long").build(),
        "[ {\"type\": \"provided\", \"name\": \"version\", \"values\": \"long\"} ]");

    TestHelpers.assertThrows("Should reject missing name",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {\"type\": \"provided\"} ]");
          }
        });
    TestHelpers.assertThrows("Should reject unsupported values type",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {" +
                "\"type\": \"provided\", " +
                "\"name\": \"version\", " +
                "\"values\": \"float\"" +
                "} ]");
          }
        });
    TestHelpers.assertThrows("Should reject invalid values type",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {" +
                "\"type\": \"provided\", " +
                "\"name\": \"version\", " +
                "\"values\": \"unknown\"" +
                "} ]");
          }
        });
  }

  @Test
  public void testMultipleFields() {
    checkParser(new PartitionStrategy.Builder()
            .provided("version")
            .hash("username", 64)
            .identity("username", "u")
            .year("time")
            .month("time")
            .day("time")
            .hour("time")
            .minute("time")
            .dateFormat("time", "datetime", "yyyy_MM_dd_HHmmss")
            .build(),
        "[ " +
            "{\"type\": \"provided\", \"name\": \"version\"}," +
            "{\"type\": \"hash\", \"source\": \"username\", \"buckets\": 64}," +
            "{\"type\": \"identity\"," +
                "\"source\": \"username\", \"name\": \"u\"}," +
            "{\"type\": \"year\", \"source\": \"time\"}," +
            "{\"type\": \"month\", \"source\": \"time\"}," +
            "{\"type\": \"day\", \"source\": \"time\"}," +
            "{\"type\": \"hour\", \"source\": \"time\"}," +
            "{\"type\": \"minute\", \"source\": \"time\"}," +
            "{\"type\": \"dateFormat\", \"source\": \"time\", " +
                "\"name\": \"datetime\", \"format\": \"yyyy_MM_dd_HHmmss\"}" +
        " ]");
  }

  @Test
  public void testNumericInsteadOfString() {
    // coerced to a string
    checkParser(new PartitionStrategy.Builder().year("34").build(),
        "[ {\"type\": \"year\", \"source\": 34} ]");
  }

  @Test
  public void testMissingSource() {
    String[] types = new String[] {
        "identity", "hash", "year", "month", "day", "hour", "minute",
        "dateFormat"};
    for (final String type : types) {
      TestHelpers.assertThrows("Should reject missing source",
          ValidationException.class, new Runnable() {
            @Override
            public void run() {
              PartitionStrategyParser.parse("[ {\"type\": \"" + type + "\"} ]");
            }
          }
      );
    }
  }

  @Test
  public void testMissingType() {
    TestHelpers.assertThrows("Should reject missing partitioner type",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {\"source\": \"banana\"} ]");
          }
        }
    );
  }

  @Test
  public void testUnknownType() {
    TestHelpers.assertThrows("Should reject unknown partitioner type",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {\"type\": \"cats\", \"source\": \"banana\"} ]");
          }
        }
    );
  }

  @Test
  public void testJsonObject() {
    TestHelpers.assertThrows("Should reject non-array strategy",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("{\"type\": \"year\", \"source\": \"banana\"}");
          }
        }
    );
  }

  @Test
  public void testNonRecordPartitioner() {
    TestHelpers.assertThrows("Should reject JSON string partitioner",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ " +
                "{\"type\": \"year\", \"source\": \"time\"}," +
                "\"cheese!\"" +
                " ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject JSON number partitioner",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ " +
                "{\"type\": \"year\", \"source\": \"time\"}," +
                "34" +
                " ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject JSON array partitioner",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ " +
                "{\"type\": \"year\", \"source\": \"time\"}," +
                "[ 1, 2, 3 ]" +
                " ]");
          }
        }
    );
  }

  @Test
  public void testInvalidJson() {
    TestHelpers.assertThrows("Should reject bad JSON",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse("[ {\"type\", \"year\", \"source\": \"banana\"} ]");
          }
        }
    );
  }

  @Test
  public void testInputStreamIOException() {
    TestHelpers.assertThrows("Should pass DatasetIOException",
        DatasetIOException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse(new InputStream() {
              @Override
              public int read() throws IOException {
                throw new IOException("InputStream angry.");
              }
            });
          }
        }
    );
  }

  @Test
  public void testMissingFile() {
    TestHelpers.assertThrows("Should pass DatasetIOException",
        DatasetIOException.class, new Runnable() {
          @Override
          public void run() {
            PartitionStrategyParser.parse(new File("target/missing.json"));
          }
        }
    );
  }

  @Test
  public void testAddEmbeddedPartitionStrategy() {
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .hash("username", 16)
        .identity("username", "u")
        .build();
    Schema original = new Schema.Parser().parse("{" +
        "  \"type\": \"record\"," +
        "  \"name\": \"User\"," +
        "  \"fields\": [" +
        "    {\"name\": \"id\", \"type\": \"long\"}," +
        "    {\"name\": \"username\", \"type\": \"string\"}," +
        "    {\"name\": \"real_name\", \"type\": \"string\"}" +
        "  ]" +
        "}");
    Schema embedded = PartitionStrategyParser.embedPartitionStrategy(original, strategy);

    Assert.assertTrue(PartitionStrategyParser.hasEmbeddedStrategy(embedded));
    Assert.assertEquals(strategy, PartitionStrategyParser.parseFromSchema(embedded));
  }

  @Test
  public void testReplaceEmbeddedPartitionStrategy() {
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .hash("username", 16)
        .identity("username", "u")
        .build();
    Schema original = new Schema.Parser().parse("{" +
        "  \"type\": \"record\"," +
        "  \"name\": \"User\"," +
        "  \"partitions\": [" +
        "    {\"type\": \"hash\", \"source\": \"real_name\", \"buckets\": 64}," +
        "    {\"type\": \"identity\", \"source\": \"real_name\", \"name\": \"r\"}" +
        "  ]," +
        "  \"fields\": [" +
        "    {\"name\": \"id\", \"type\": \"long\"}," +
        "    {\"name\": \"username\", \"type\": \"string\"}," +
        "    {\"name\": \"real_name\", \"type\": \"string\"}" +
        "  ]" +
        "}");
    Assert.assertTrue(PartitionStrategyParser.hasEmbeddedStrategy(original));
    Assert.assertFalse(PartitionStrategyParser.parseFromSchema(original).equals(strategy));

    Schema embedded = PartitionStrategyParser.embedPartitionStrategy(original, strategy);

    Assert.assertTrue(PartitionStrategyParser.hasEmbeddedStrategy(embedded));
    Assert.assertEquals(strategy, PartitionStrategyParser.parseFromSchema(embedded));
  }
}
