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

package org.kitesdk.cli;

import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.List;
import org.apache.avro.Schema;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.internal.matchers.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtil {
  public static int run(String... args) throws Exception {
    Logger console = LoggerFactory.getLogger(Main.class);
    return run(console, args);
  }

  public static int run(Logger console, String... args) throws Exception {
    return new Main(console).run(args);
  }

  public static Matcher<String> matchesMinimizedSchema(Schema schema) {
    return new SchemaMatcher(schema, true);
  }

  public static Matcher<String> matchesSchema(Schema schema) {
    return new SchemaMatcher(schema, false);
  }

  private static class SchemaMatcher extends TypeSafeMatcher<String> {
    private final Schema expected;
    private final boolean minimized;

    public SchemaMatcher(Schema expected, boolean minimized) {
      this.expected = expected;
      this.minimized = minimized;
    }

    @Override
    public boolean matchesSafely(String s) {
      if (minimized && s.contains("\n")) {
        return false;
      } else if (!minimized && !s.contains("\n")) {
        return false;
      }
      Schema schema = new Schema.Parser().parse(s);
      return (new SchemaComparator().compare(expected, schema) == 0);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText(minimized ? "Minimized" : "Full")
          .appendText(" schema equivalent to: ")
          .appendText(expected.toString(!minimized));
    }
  }

  public static class SchemaComparator implements Comparator<Schema> {
    private final ListComparator<Schema.Field> fields =
        new ListComparator<Schema.Field>(new Comparator<Schema.Field>() {
          @Override
          public int compare(Schema.Field f1, Schema.Field f2) {
            return SchemaComparator.this.compare(f1.schema(), f2.schema());
          }
        });
    private final ListComparator<Schema> schemas =
        new ListComparator<Schema>(this);
    private final ListComparator<String> strings =
        new ListComparator<String>(String.class);

    @Override
    public int compare(Schema s1, Schema s2) {
      // compare types
      int cmp = s1.getType().compareTo(s2.getType());
      if (cmp != 0) {
        return cmp;
      }

      // compare names
      if (s1.getName() != null) {
        if (s2.getName() == null) {
          return -1;
        }
        cmp = s1.getName().compareTo(s2.getName());
        if (cmp != 0) {
          return cmp;
        }
      } else if (s2.getName() != null) {
        return 1;
      }

      // compare child types
      switch (s1.getType()) {
        case RECORD:
          return fields.compare(s1.getFields(), s2.getFields());
        case UNION:
          return schemas.compare(s1.getTypes(), s2.getTypes());
        case ENUM:
          return strings.compare(s1.getEnumSymbols(), s2.getEnumSymbols());
        case ARRAY:
          return compare(s1.getElementType(), s2.getElementType());
        case MAP:
          return compare(s1.getValueType(), s2.getValueType());
      }
      return 0;
    }
  }

  public static class ListComparator<T> implements Comparator<List<T>> {
    private final Comparator<T> itemComparator;

    @SuppressWarnings("unchecked")
    public ListComparator(Class<T> itemClass) {
      Preconditions.checkArgument(Comparable.class.isAssignableFrom(itemClass),
          "Items must be Comparable");
      itemComparator = (Comparator<T>) new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          return o1.compareTo(o2);
        }
      };
    }

    public ListComparator(Comparator<T> itemComparator) {
      this.itemComparator = itemComparator;
    }

    @Override
    public int compare(List<T> l1, List<T> l2) {
      int cmp = Integer.compare(l1.size(), l2.size());
      if (cmp != 0) {
        return cmp;
      }
      for (int i = 0; i < l1.size(); i += 1) {
        cmp = itemComparator.compare(l1.get(i), l2.get(i));
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    }
  }
}
