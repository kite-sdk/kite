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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestSchemaValidationUtil {

  @Test
  public void testAllAvroTypes() {
    Schema r = SchemaBuilder.record("r").fields()
        .requiredBoolean("boolF")
        .requiredInt("intF")
        .requiredLong("longF")
        .requiredFloat("floatF")
        .requiredDouble("doubleF")
        .requiredString("stringF")
        .requiredBytes("bytesF")
        .name("fixedF1").type().fixed("F1").size(1).noDefault()
        .name("enumF").type().enumeration("E1").symbols("S").noDefault()
        .name("mapF").type().map().values().stringType().noDefault()
        .name("arrayF").type().array().items().stringType().noDefault()
        .name("recordF").type().record("inner").fields()
          .name("f").type().intType().noDefault()
          .endRecord().noDefault()
        .optionalBoolean("boolO")
        .endRecord();
    assertTrue(SchemaValidationUtil.canRead(r, r));
  }
}
