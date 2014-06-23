/**
 * Copyright 2013 Cloudera Inc.
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
package org.kitesdk.data.impl;

import org.kitesdk.data.Format;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.PartitionStrategy;

/**
 * <p>
 * Class to enforce "friend" access to internal methods in
 * {@link org.kitesdk.data} classes that are not a part of the public API.
 * </p>
 * <p>
 * This technique is described in detail in "Practical API Design" by
 * Jaroslav Tulach.
 * </p>
 */
public abstract class Accessor {
  private static volatile Accessor DEFAULT;
  public static Accessor getDefault() {
    Accessor a = DEFAULT;
    if (a != null) {
      return a;
    }
    try {
      Class.forName(PartitionStrategy.class.getName(), true,
          PartitionStrategy.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return DEFAULT;
  }

  public static void setDefault(Accessor accessor) {
    if (DEFAULT != null) {
      throw new IllegalStateException();
    }
    DEFAULT = accessor;
  }

  public Accessor() {
  }

  public abstract Format newFormat(String name);

  public abstract PartitionKey newPartitionKey(Object... values);

  public abstract PartitionStrategy getSubpartitionStrategy(PartitionStrategy partitionStrategy, int startIndex);

  public abstract String toExpression(PartitionStrategy partitionStrategy);

  public abstract PartitionStrategy fromExpression(String partitionExpression);

}
