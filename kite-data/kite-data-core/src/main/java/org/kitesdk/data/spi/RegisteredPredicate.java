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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;

public abstract class RegisteredPredicate<T> implements Predicate<T> {

  private static final Pattern NAME = Pattern.compile("\\w+");
  private static final Pattern FUNCTION = Pattern.compile("(\\w+)\\(([^\\)*])\\)");

  public static interface Factory {
    public <T> RegisteredPredicate<T> fromString(String predicate, Schema schema);
  }

  private static final Map<String, Factory> REGISTRY = Maps.newHashMap();

  public static void register(String name, Factory factory) {
    Preconditions.checkArgument(NAME.matcher(name).matches(),
        "Invalid name, must be alphanumeric (plus _): " + name);
    REGISTRY.put(name, factory);
  }

  public static String toString(RegisteredPredicate<?> predicate, Schema schema) {
    return predicate.getName() + "(" + predicate.toString(schema) + ")";
  }

  public static <T> RegisteredPredicate<T> fromString(String predicate, Schema schema) {
    Matcher match = FUNCTION.matcher(predicate);
    if (match.matches()) {
      return REGISTRY.get(match.group(1)).fromString(match.group(2), schema);
    } else {
      return null;
    }
  }

  public abstract String getName();
  public abstract String toString(Schema schema);
}
