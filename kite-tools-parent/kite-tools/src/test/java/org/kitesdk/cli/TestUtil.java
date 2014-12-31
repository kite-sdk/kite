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
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.internal.matchers.TypeSafeMatcher;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtil {
  private static Logger newConsole() {
    return LoggerFactory.getLogger(Main.class);
  }

  public static int run(String... args) throws Exception {
    return run(newConsole(), args);
  }

  public static int run(Logger console, String... args) throws Exception {
    return run(console, DefaultConfiguration.get(), args);
  }

  public static int run(Configuration conf, String... args) throws Exception {
    return run(newConsole(), conf, args);
  }

  public static int run(Logger console, Configuration conf, String... args)
      throws Exception {
    // ensure the default config is not changed by calling Main
    Configuration original = DefaultConfiguration.get();
    Main main = new Main(console);
    main.setConf(conf);
    int rc = main.run(args);
    DefaultConfiguration.set(original);
    return rc;
  }

  public static Matcher<DatasetDescriptor> matches(DatasetDescriptor desc) {
    return new DescriptorMatcher(desc);
  }

  public static Matcher<String> matchesMinimizedSchema(Schema schema) {
    return new SchemaMatcher(schema, true);
  }

  public static Matcher<String> matchesSchema(Schema schema) {
    return new SchemaMatcher(schema, false);
  }

  public static Matcher<String> matchesPattern(String pattern) {
    return new PatternMatcher(pattern);
  }

  public static void assertMatches(String pattern, String actual) {
    Assert.assertTrue("Expected:\n\n" + pattern + "\n\n" + "\tbut was:\n" +
        actual + "\n", matchesPattern(pattern).matches(actual));
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
      int s1 = l1.size();
      int s2 = l2.size();
      int cmp = (s1 < s2) ? -1 : ((s1 == s2) ? 0 : 1);
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

  private static class DescriptorMatcher extends TypeSafeMatcher<DatasetDescriptor> {
    private final DatasetDescriptor descriptor;

    public DescriptorMatcher(DatasetDescriptor descriptor) {
      this.descriptor = descriptor;
    }

    @Override
    public boolean matchesSafely(DatasetDescriptor actual) {
      return descriptor.equals(actual);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("DatasetDescriptor equivalent to ")
          .appendText(descriptor.toString());
    }
  }

  private static class PatternMatcher extends TypeSafeMatcher<String> {
    private final Pattern pattern;

    public PatternMatcher(String pattern) {
      this.pattern = Pattern.compile(pattern);
    }

    @Override
    public boolean matchesSafely(String item) {
      return pattern.matcher(item).matches();
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("String matches ").appendText(pattern.toString());
    }

  }
}
