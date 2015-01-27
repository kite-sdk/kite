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

package org.kitesdk.data.spi;

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaUtil {

  Schema optionalSchema = SchemaBuilder.record("Test").fields()
      .requiredLong("id")
      .optionalString("content")
      .endRecord();

  Schema requiredSchema = SchemaBuilder.record("Test").fields()
      .requiredLong("id")
      .requiredString("content")
      .endRecord();

  Schema typeChangeSchema = SchemaBuilder.record("Test").fields()
      .requiredInt("id")
      .optionalString("content")
      .endRecord();

  Schema renamedRecordSchema = SchemaBuilder.record("TEST").fields()
      .requiredLong("id")
      .optionalString("contEnt")
      .endRecord();

  Schema renamedFieldSchema = SchemaBuilder.record("Test").fields()
      .requiredLong("id")
      .optionalString("CONTENT")
      .endRecord();

  @Test
  public void testRecordSchemaDigest() {
    // to check that each schema change results in a new digest
    Set<String> digests = Sets.newHashSet();

    SchemaUtil.Digest digest = SchemaUtil.digest(optionalSchema);
    Assert.assertEquals(40, digest.toString().length());
    digests.add(digest.toString());
    Assert.assertEquals("Should produce the same digest for the schema",
        SchemaUtil.digest(deepCopy(optionalSchema)).toString(),
        digest.toString());

    SchemaUtil.Digest requiredDigest = SchemaUtil.digest(requiredSchema);
    Assert.assertEquals(40, requiredDigest.toString().length());
    digests.add(requiredDigest.toString());
    Assert.assertEquals("Should produce the same digest for the schema",
        SchemaUtil.digest(deepCopy(requiredSchema)).toString(),
        requiredDigest.toString());

    SchemaUtil.Digest typeChangeDigest = SchemaUtil.digest(typeChangeSchema);
    Assert.assertEquals(40, typeChangeDigest.toString().length());
    digests.add(typeChangeDigest.toString());
    Assert.assertEquals("Should produce the same digest for the schema",
        SchemaUtil.digest(deepCopy(typeChangeSchema)).toString(),
        typeChangeDigest.toString());

    SchemaUtil.Digest renamedRecordDigest = SchemaUtil.digest(renamedRecordSchema);
    Assert.assertEquals(40, renamedRecordDigest.toString().length());
    digests.add(renamedRecordDigest.toString());
    Assert.assertEquals("Should produce the same digest for the schema",
        SchemaUtil.digest(deepCopy(renamedRecordSchema)).toString(),
        renamedRecordDigest.toString());

    SchemaUtil.Digest renamedFieldDigest = SchemaUtil.digest(renamedFieldSchema);
    Assert.assertEquals(40, renamedFieldDigest.toString().length());
    digests.add(renamedFieldDigest.toString());
    Assert.assertEquals("Should produce the same digest for the schema",
        SchemaUtil.digest(deepCopy(renamedFieldSchema)).toString(),
        renamedFieldDigest.toString());

    Assert.assertEquals("Should produce different digests", 5, digests.size());
  }

  private Schema deepCopy(Schema schema) {
    return new Schema.Parser().parse(schema.toString());
  }
}
