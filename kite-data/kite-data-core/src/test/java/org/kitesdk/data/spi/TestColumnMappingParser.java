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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.ValidationException;

public class TestColumnMappingParser {

  public static void checkParser(ColumnMapping expected, String json) {
    ColumnMapping parsed = ColumnMappingParser.parse(json);
    Assert.assertEquals(expected, parsed);

    parsed = ColumnMappingParser.parse(expected.toString());
    Assert.assertEquals("Should reparse properly", expected, parsed);
  }

  @Test
  public void testMultipleMappings() {
    checkParser(new ColumnMapping.Builder()
        .key("key_field")
        .column("username", "meta", "username")
        .column("email", "meta", "email")
        .keyAsColumn("map_field", "map_content", "key_")
        .counter("counter_field", "meta", "counter_field")
        .counter("counter_field2", "meta", "counter_field2")
        .build(),
        "[\n" +
        "{\"source\": \"key_field\", \"type\": \"key\"},\n" +
        "{\"source\": \"username\", \"type\": \"column\"," +
            "\"family\": \"meta\", \"qualifier\": \"username\"},\n" +
        "{\"source\": \"email\", \"type\": \"column\"," +
            "\"family\": \"meta\", \"qualifier\": \"email\"},\n" +
        "{\"source\": \"map_field\", \"type\": \"keyAsColumn\"," +
            "\"family\": \"map_content\", \"prefix\": \"key_\"},\n" +
        "{\"source\": \"counter_field\", \"type\": \"counter\"," +
            "\"family\": \"meta\", \"qualifier\": \"counter_field\"},\n" +
        "{\"source\": \"counter_field2\", \"type\": \"counter\"," +
            "\"family\": \"meta\", \"qualifier\": \"counter_field2\"}\n" +
        "]\n");
    checkParser(new ColumnMapping.Builder()
            .key("key_field")
            .column("username", "meta", "username")
            .column("email", "meta", "email")
            .keyAsColumn("map_field", "map_content", "key_")
            .version("version")
            .build(),
        "[\n" +
        "{\"source\": \"key_field\", \"type\": \"key\"},\n" +
        "{\"source\": \"username\", \"type\": \"column\"," +
            "\"family\": \"meta\", \"qualifier\": \"username\"},\n" +
        "{\"source\": \"email\", \"type\": \"column\"," +
            "\"family\": \"meta\", \"qualifier\": \"email\"},\n" +
        "{\"source\": \"map_field\", \"type\": \"keyAsColumn\"," +
            "\"family\": \"map_content\", \"prefix\": \"key_\"},\n" +
        "{\"source\": \"version\", \"type\": \"occVersion\"}\n" +
        "]\n");

    TestHelpers.assertThrows("Should reject counter and occVersion",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[" +
                "{\"source\": \"v\", \"type\": \"occVersion\"}," +
                "{\"source\": \"c\", \"type\": \"counter\", \"value\": \"f:q\"}" +
                "]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject more than one occVersion",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[" +
                "{\"source\": \"v\", \"type\": \"occVersion\"}," +
                "{\"source\": \"v2\", \"type\": \"occVersion\"}" +
                "]");
          }
        }
    );
  }

  @Test
  public void testKeyMapping() {
    checkParser(new ColumnMapping.Builder()
            .key("s")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"key\"} ]");
    TestHelpers.assertThrows("Should reject missing source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"type\": \"key\"} ]");
          }
        }
    );
  }

  @Test
  public void testColumnMapping() {
    checkParser(new ColumnMapping.Builder()
            .column("s", "f", "q")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"column\", \"value\": \"f:q\"} ]");
    checkParser(new ColumnMapping.Builder()
            .column("s", "f", "q")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"column\"," +
            "\"family\": \"f\", \"qualifier\": \"q\"} ]");
    // if both value and family/qualifier are present, family/qualifier wins
    checkParser(new ColumnMapping.Builder()
            .column("s", "fam", "qual")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"column\", \"value\": \"f:q\", " +
            "\"family\": \"fam\", \"qualifier\": \"qual\"} ]");

    TestHelpers.assertThrows("Should reject missing source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"type\": \"column\", \"family\": \"f\"," +
                "\"qualifier\": \"q\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject missing qualifier",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", " +
                "\"type\": \"column\", \"value\": \"f\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty qualifier",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", " +
                "\"type\": \"column\", \"value\": \"f:\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject missing qualifier",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", " +
                "\"type\": \"column\", \"family\": \"f\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject missing qualifier",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"column\"," +
                "\"family\": \"f\", \"qualifier\": \"\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject missing family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"column\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"column\"" +
                "\"value\": \"\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"column\"" +
                "\"value\": \":\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"column\"" +
                "\"family\": \"\"} ]");
          }
        }
    );
  }

  @Test
  public void testKeyAsColumnMapping() {
    checkParser(new ColumnMapping.Builder()
            .keyAsColumn("s", "f")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"keyAsColumn\", \"value\": \"f\"} ]");
    checkParser(new ColumnMapping.Builder()
            .keyAsColumn("s", "f", "p")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"keyAsColumn\"," +
            "\"value\": \"f:p\"} ]");

    checkParser(new ColumnMapping.Builder()
            .keyAsColumn("s", "f")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"keyAsColumn\"," +
            "\"family\": \"f\" } ]");
    checkParser(new ColumnMapping.Builder()
            .keyAsColumn("s", "f", "p")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"keyAsColumn\"," +
            "\"family\": \"f\", \"prefix\": \"p\" } ]");
    // if both family is present, family wins
    checkParser(new ColumnMapping.Builder()
            .keyAsColumn("s", "fam")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"keyAsColumn\", \"value\": \"f\", " +
            "\"family\": \"fam\"} ]");
    // prefix wins
    checkParser(new ColumnMapping.Builder()
            .keyAsColumn("s", "fam", "pre")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"keyAsColumn\"," +
            "\"value\": \"f:p\", \"family\": \"fam\", \"prefix\": \"pre\"} ]");

    TestHelpers.assertThrows("Should reject missing source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"type\": \"keyAsColumn\", \"family\": \"f\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject missing family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"keyAsColumn\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"keyAsColumn\"" +
                "\"value\": \"\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"keyAsColumn\"" +
                "\"family\": \"\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"keyAsColumn\"" +
                "\"value\": \":\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject qualifier",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"keyAsColumn\"," +
                "\"family\": \"f\", \"qualifier\": \"\"} ]");
          }
        }
    );
  }

  @Test
  public void testCounterMapping() {
    checkParser(new ColumnMapping.Builder()
            .counter("s", "f", "q")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"counter\", \"value\": \"f:q\"} ]");
    checkParser(new ColumnMapping.Builder()
            .counter("s", "f", "q")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"counter\"," +
            "\"family\": \"f\", \"qualifier\": \"q\"} ]"
    );
    // if both value and family/qualifier are present, family/qualifier wins
    checkParser(new ColumnMapping.Builder()
            .counter("s", "fam", "qual")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"counter\", \"value\": \"f:q\", " +
            "\"family\": \"fam\", \"qualifier\": \"qual\"} ]");

    TestHelpers.assertThrows("Should reject missing source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"type\": \"counter\", \"family\": \"f\"," +
                "\"qualifier\": \"q\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject missing qualifier",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", " +
                "\"type\": \"counter\", \"value\": \"f\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty qualifier",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", " +
                "\"type\": \"counter\", \"value\": \"f:\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject missing qualifier",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", " +
                "\"type\": \"counter\", \"family\": \"f\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject missing qualifier",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"counter\"," +
                "\"family\": \"f\", \"qualifier\": \"\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject missing family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"counter\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"counter\"" +
                "\"value\": \"\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"counter\"" +
                "\"value\": \":\"} ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject empty family",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"s\", \"type\": \"counter\"" +
                "\"family\": \"\"} ]");
          }
        }
    );
  }

  @Test
  public void testOCCVersionMapping() {
    checkParser(new ColumnMapping.Builder()
            .occ("s")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"occVersion\"} ]");
    checkParser(new ColumnMapping.Builder()
            .version("s")
            .build(),
        "[ {\"source\": \"s\", \"type\": \"occVersion\"} ]");
    TestHelpers.assertThrows("Should reject missing source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"type\": \"occVersion\"} ]");
          }
        }
    );
  }

  @Test
  public void testNumericInsteadOfString() {
    // coerced to a string
    checkParser(new ColumnMapping.Builder().key("34").build(),
        "[ {\"type\": \"key\", \"source\": 34} ]");
  }

  @Test
  public void testMissingType() {
    TestHelpers.assertThrows("Should reject missing mapping type",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"source\": \"banana\"} ]");
          }
        }
    );
  }

  @Test
  public void testUnknownType() {
    TestHelpers.assertThrows("Should reject unknown mapping type",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ {\"type\": \"cats\", \"source\": \"banana\"} ]");
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
            ColumnMappingParser.parse("{\"type\": \"year\", \"source\": \"banana\"}");
          }
        }
    );
  }

  @Test
  public void testNonRecordMapping() {
    TestHelpers.assertThrows("Should reject JSON string mapping",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ " +
                "{\"type\": \"key\", \"source\": \"id\"}," +
                "\"cheese!\"" +
                " ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject JSON number mapping",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ " +
                "{\"type\": \"key\", \"source\": \"id\"}," +
                "34" +
                " ]");
          }
        }
    );
    TestHelpers.assertThrows("Should reject JSON array mapping",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            ColumnMappingParser.parse("[ " +
                "{\"type\": \"key\", \"source\": \"id\"}," +
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
            ColumnMappingParser.parse("[ {\"type\", \"key\", \"source\": \"banana\"} ]");
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
            ColumnMappingParser.parse(new InputStream() {
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
            ColumnMappingParser.parse(new File("target/missing.json"));
          }
        }
    );
  }

  @Test
  public void testAddEmbeddedColumnMapping() {
    ColumnMapping mapping = new ColumnMapping.Builder()
        .key("id")
        .column("username", "u", "username")
        .column("real_name", "u", "name")
        .build();
    Schema original = new Schema.Parser().parse("{" +
        "  \"type\": \"record\"," +
        "  \"name\": \"User\"," +
        "  \"partitions\": [" +
        "    {\"type\": \"identity\", \"source\": \"id\", \"name\": \"id_copy\"}" +
        "  ]," +
        "  \"fields\": [" +
        "    {\"name\": \"id\", \"type\": \"long\"}," +
        "    {\"name\": \"username\", \"type\": \"string\"}," +
        "    {\"name\": \"real_name\", \"type\": \"string\"}" +
        "  ]" +
        "}");

    Schema embedded = ColumnMappingParser.embedColumnMapping(original, mapping);

    junit.framework.Assert.assertTrue(ColumnMappingParser.hasEmbeddedColumnMapping(embedded));
    junit.framework.Assert.assertEquals(mapping, ColumnMappingParser.parseFromSchema(embedded));
  }

  @Test
  public void testReplaceEmbeddedPartitionStrategy() {
    ColumnMapping mapping = new ColumnMapping.Builder()
        .key("id")
        .column("username", "u", "username")
        .column("real_name", "u", "name")
        .build();
    Schema original = new Schema.Parser().parse("{" +
        "  \"type\": \"record\"," +
        "  \"name\": \"User\"," +
        "  \"partitions\": [" +
        "    {\"type\": \"identity\", \"source\": \"id\", \"name\": \"id_copy\"}" +
        "  ]," +
        "  \"mapping\": [" +
        "    {\"type\": \"occVersion\", \"source\": \"id\"}," +
        "    {\"type\": \"column\"," +
        "     \"source\": \"username\"," +
        "     \"family\": \"meta\"," +
        "     \"qualifier\": \"u\"}," +
        "    {\"type\": \"column\"," +
        "     \"source\": \"real_name\"," +
        "     \"family\": \"meta\"," +
        "     \"qualifier\": \"r\"}" +
        "  ]," +
        "  \"fields\": [" +
        "    {\"name\": \"id\", \"type\": \"long\"}," +
        "    {\"name\": \"username\", \"type\": \"string\"}," +
        "    {\"name\": \"real_name\", \"type\": \"string\"}" +
        "  ]" +
        "}");
    Assert.assertTrue(ColumnMappingParser.hasEmbeddedColumnMapping(original));
    Assert.assertFalse(ColumnMappingParser.parseFromSchema(original).equals(mapping));

    Schema embedded = ColumnMappingParser.embedColumnMapping(original, mapping);

    Assert.assertTrue(ColumnMappingParser.hasEmbeddedColumnMapping(embedded));
    Assert.assertEquals(mapping, ColumnMappingParser.parseFromSchema(embedded));
  }

  @Test
  public void testGetKeyMappingsFromSchemaFields() {
    Schema schema = new Schema.Parser().parse("{" +
        "  \"type\": \"record\"," +
        "  \"name\": \"User\"," +
        "  \"fields\": [" +
        "    {\"name\": \"id\", \"type\": \"long\", \"mapping\":" +
        "      {\"type\": \"key\", \"value\": \"1\"} }," +
        "    {\"name\": \"username\", \"type\": \"string\", \"mapping\":" +
        "      {\"type\": \"key\", \"value\": \"0\"} }," +
        "    {\"name\": \"real_name\", \"type\": \"string\", \"mapping\":" +
        "      {\"type\": \"column\", \"value\": \"m:name\"} }" +
        "  ]" +
        "}");
    Map<Integer, FieldMapping> keys = ColumnMappingParser
        .parseKeyMappingsFromSchemaFields(schema);
    ImmutableMap expected = ImmutableMap.builder()
        .put(0, FieldMapping.key("username"))
        .put(1, FieldMapping.key("id"))
        .build();
    Assert.assertEquals(expected, keys);
  }

}
