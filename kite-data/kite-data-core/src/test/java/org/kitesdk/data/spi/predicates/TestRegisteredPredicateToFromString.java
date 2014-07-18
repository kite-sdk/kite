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

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.avro.generic.GenericData.Record;

public class TestRegisteredPredicateToFromString {
  private static final Schema SCHEMA = SchemaBuilder.record("Event").fields()
      .requiredString("id")
      .requiredLong("timestamp")
      .requiredString("color")
      .endRecord();

  private static final Schema STRING = Schema.create(Schema.Type.STRING);

  @Test
  public void testExists() {
    Exists<Record> exists = Predicates.exists();
    Assert.assertEquals("", exists.toString(SCHEMA));
    Assert.assertEquals(
        "exists()", RegisteredPredicate.toString(exists, SCHEMA));
    Assert.assertEquals(
        exists, RegisteredPredicate.<Record>fromString("exists()", SCHEMA));
  }

  /**
   * A test RegisteredPredicate. Do not use this class elsewhere because it
   * uses toString rather than supporting CharSequences directly.
   */
  public static class Contains<T> extends RegisteredPredicate<T> {
    static {
      RegisteredPredicate.register("contains", new Factory() {
        @Override
        public <T> RegisteredPredicate<T> fromString(String predicate, Schema schema) {
          return new Contains<T>(predicate);
        }
      });
    }

    private final String contained;

    public Contains(String contained) {
      this.contained = contained;
    }

    @Override
    public String getName() {
      return "contains";
    }

    @Override
    public String toString(Schema schema) {
      return contained;
    }

    @Override
    public boolean apply(@Nullable T value) {
      return value != null &&  value.toString().contains(contained);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(contained);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      Contains other = (Contains) obj;
      return Objects.equals(contained, other.contained);
    }
  }

  public static Contains<String> contains(String contained) {
    return new Contains<String>(contained);
  }

  @Test
  public void testContains() {
    Contains<String> a = contains("a");
    Contains<String> b = contains("b");
    Assert.assertEquals("Should wrap delegate toString in name function",
        "contains(a)", RegisteredPredicate.toString(a, STRING));
    Assert.assertEquals("Should wrap delegate toString in name function",
        "contains(b)", RegisteredPredicate.toString(b, STRING));
    Assert.assertEquals("Should produce equivalent contains(a)",
        a, RegisteredPredicate.<String>fromString("contains(a)", STRING));
  }

}
