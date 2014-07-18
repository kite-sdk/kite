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

package org.kitesdk.data.spi.predicates;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

public class TestRangeToFromString {
  private static final Schema STRING = Schema.create(Schema.Type.STRING);
  private static final Schema INT = Schema.create(Schema.Type.INT);
  private static final Schema LONG = Schema.create(Schema.Type.LONG);
  private static final Schema FLOAT = Schema.create(Schema.Type.FLOAT);
  private static final Schema DOUBLE = Schema.create(Schema.Type.DOUBLE);

  @Test
  public void testRangeBoundTypes() {
    Assert.assertEquals("Should use [ and ]",
        "[3,4]", Ranges.closed(3,4).toString(INT));
    Assert.assertEquals("Should produce closed endpoints",
        Ranges.closed(3,4), Range.<Integer>fromString("[3,4]", INT));
    Assert.assertEquals("Should use ( and )",
        "(5,9)", Ranges.open(5,9).toString(INT));
    Assert.assertEquals("Should produce open endpoints",
        Ranges.open(5,9), Range.<Integer>fromString("(5,9)", INT));
    Assert.assertEquals("Should use [ and )",
        "[3,4)", Ranges.closedOpen(3,4).toString(INT));
    Assert.assertEquals("Should produce closed, open endpoints",
        Ranges.closedOpen(3,4), Range.<Integer>fromString("[3,4)", INT));
    Assert.assertEquals("Should use ( and ]",
        "(5,9]", Ranges.openClosed(5,9).toString(INT));
    Assert.assertEquals("Should produce open, closed endpoints",
        Ranges.openClosed(5,9), Range.<Integer>fromString("(5,9]", INT));
  }

  @Test
  public void testStringRangeBoundTypes() {
    Assert.assertEquals("Should use [ and ]",
        "[c,f]", Ranges.closed("c", "f").toString(STRING));
    Assert.assertEquals("Should produce closed endpoints",
        Ranges.closed("c","f"), Range.<String>fromString("[c,f]", STRING));
    Assert.assertEquals("Should use ( and )",
        "(m,p)", Ranges.open("m", "p").toString(STRING));
    Assert.assertEquals("Should produce open endpoints",
        Ranges.open("m", "p"), Range.<String>fromString("(m,p)", STRING));
    Assert.assertEquals("Should use [ and )",
        "[c,f)", Ranges.closedOpen("c", "f").toString(STRING));
    Assert.assertEquals("Should produce closed, open endpoints",
        Ranges.closedOpen("c", "f"), Range.<String>fromString("[c,f)", STRING));
    Assert.assertEquals("Should use ( and ]",
        "(m,p]", Ranges.openClosed("m", "p").toString(STRING));
    Assert.assertEquals("Should produce open, closed endpoints",
        Ranges.openClosed("m", "p"), Range.<String>fromString("(m,p]", STRING));
  }

  @Test
  public void testRangeUnboundedTypes() {
    Assert.assertEquals("Should use [ and inf)",
        "[3,inf)", Ranges.atLeast(3).toString(INT));
    Assert.assertEquals("Should parse inf) correctly",
        Ranges.atLeast(3), Range.<Integer>fromString("[3,inf)", INT));
    Assert.assertEquals("Should use ( and inf)",
        "(5,inf)", Ranges.greaterThan(5).toString(INT));
    Assert.assertEquals("Should parse inf) correctly",
        Ranges.greaterThan(5), Range.<Integer>fromString("(5,inf)", INT));
    Assert.assertEquals("Should use (inf and )",
        "(inf,4)", Ranges.lessThan(4).toString(INT));
    Assert.assertEquals("Should parse (inf correctly",
        Ranges.lessThan(4), Range.<Integer>fromString("(inf,4)", INT));
    Assert.assertEquals("Should use (inf and ]",
        "(inf,9]", Ranges.atMost(9).toString(INT));
    Assert.assertEquals("Should parse (inf correctly",
        Ranges.atMost(9), Range.<Integer>fromString("(inf,9]", INT));

    // accepts inclusive inf bounds
    Assert.assertEquals("Should parse inf] as inf)",
        Ranges.atLeast(3), Range.<Integer>fromString("[3,inf]", INT));
    Assert.assertEquals("Should parse [inf as (inf",
        Ranges.atMost(9), Range.<Integer>fromString("[inf,9]", INT));
  }

  @Test
  public void testStringRangeUnboundedTypes() {
    Assert.assertEquals("Should use [ and inf)",
        "[c,inf)", Ranges.atLeast("c").toString(STRING));
    Assert.assertEquals("Should parse inf) correctly",
        Ranges.atLeast("c"), Range.<String>fromString("[c,inf)", STRING));
    Assert.assertEquals("Should use ( and inf)",
        "(m,inf)", Ranges.greaterThan("m").toString(STRING));
    Assert.assertEquals("Should parse inf) correctly",
        Ranges.greaterThan("m"), Range.<String>fromString("(m,inf)", STRING));
    Assert.assertEquals("Should use (inf and )",
        "(inf,f)", Ranges.lessThan("f").toString(STRING));
    Assert.assertEquals("Should parse (inf correctly",
        Ranges.lessThan("f"), Range.<String>fromString("(inf,f)", STRING));
    Assert.assertEquals("Should use (inf and ]",
        "(inf,p]", Ranges.atMost("p").toString(STRING));
    Assert.assertEquals("Should parse (inf correctly",
        Ranges.atMost("p"), Range.<String>fromString("(inf,p]", STRING));

    // accepts inclusive inf bounds
    Assert.assertEquals("Should parse inf] as inf)",
        Ranges.atLeast("c"), Range.<String>fromString("[c,inf]", STRING));
    Assert.assertEquals("Should parse [inf as (inf",
        Ranges.atMost("p"), Range.<String>fromString("[inf,p]", STRING));
  }

  @Test
  public void testStringRanges() {
    Assert.assertEquals("Range<Utf8>#toString(STRING)",
        "[t,z)", Ranges.closedOpen(new Utf8("t"), new Utf8("z")).toString(STRING));
    Assert.assertEquals("Range#toString(STRING)",
        "[t,z)", Ranges.closedOpen("t", "z").toString(STRING));
    Assert.assertEquals("Range.fromString(String, STRING)",
        Ranges.closedOpen("t", "z"), Range.<String>fromString("[t,z)", STRING));
  }

  @Test
  public void testIntegerRanges() {
    Assert.assertEquals("Range#toString(INT)",
        "[1,7)", Ranges.closedOpen(1,7).toString(INT));
    Assert.assertEquals("Range.fromString(String, INT)",
        Ranges.closedOpen(1, 7), Range.<Integer>fromString("[1,7)", INT));
  }

  @Test
  public void testLongRanges() {
    Assert.assertEquals("Range#toString(LONG)",
        "[1,7)", Ranges.closedOpen(1l,7l).toString(LONG));
    Assert.assertEquals("Range.fromString(String, LONG)",
        Ranges.closedOpen(1l, 7l), Range.<Long>fromString("[1,7)", LONG));
  }

  @Test
  public void testFloatRanges() {
    Assert.assertEquals("Range#toString(FLOAT)",
        "[1.4,7.1)", Ranges.closedOpen(1.4f, 7.1f).toString(FLOAT));
    Assert.assertEquals("Range.fromString(String, FLOAT)",
        Ranges.closedOpen(1.4f, 7.1f), Range.<Float>fromString("[1.4,7.1)", FLOAT));
  }

  @Test
  public void testDoubleRanges() {
    Assert.assertEquals("Range#toString(DOUBLE)",
        "[1.4,7.1)", Ranges.closedOpen(1.4, 7.1).toString(DOUBLE));
    Assert.assertEquals("Range.fromString(String, DOUBLE)",
        Ranges.closedOpen(1.4, 7.1), Range.<Double>fromString("[1.4,7.1)", DOUBLE));
  }
}
