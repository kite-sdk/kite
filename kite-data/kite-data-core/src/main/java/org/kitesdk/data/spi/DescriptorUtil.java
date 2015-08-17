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

import org.kitesdk.data.DatasetDescriptor;

public class DescriptorUtil {

  /**
   * Returns whether the value of the descriptor property is {@code true}.
   *
   * @param property a String property name
   * @param descriptor a {@link DatasetDescriptor}
   * @return {@code true} if set and "true", {@code false} otherwise.
   */
  public static boolean isEnabled(String property, DatasetDescriptor descriptor) {
    if (descriptor.hasProperty(property)) {
      // return true if and only if the property value is "true"
      return Boolean.valueOf(descriptor.getProperty(property));
    }
    return false;
  }

  /**
   * Returns whether the value of the descriptor property is {@code false}.
   *
   * @param property a String property name
   * @param descriptor a {@link DatasetDescriptor}
   * @return {@code true} if set and "false", {@code false} otherwise.
   */
  public static boolean isDisabled(String property, DatasetDescriptor descriptor) {
    if (descriptor.hasProperty(property)) {
      // return true if and only if the property value is "false"
      return !Boolean.valueOf(descriptor.getProperty(property));
    }
    return false;
  }

  /**
   * Returns the value of the property parsed as a long, or the default value.
   * <p>
   * If the value cannot be parsed as a long, this will return the default
   * value.
   *
   * @param prop a String property name
   * @param descriptor a {@link DatasetDescriptor}
   * @param defaultValue default value if prop is not present or is invalid
   * @return the value of prop parsed as a long or the default value
   */
  public static long getLong(String prop, DatasetDescriptor descriptor,
                             long defaultValue) {
    if (descriptor.hasProperty(prop)) {
      String asString = descriptor.getProperty(prop);
      try {
        return Long.parseLong(asString);
      } catch (NumberFormatException e) {
        // return the default value
      }
    }
    return defaultValue;
  }

  /**
   * Returns the value of the property parsed as an int, or the default value.
   * <p>
   * If the value cannot be parsed as an int or if the value is larger than the
   * maximum value of an int, this will return the default value.
   *
   * @param prop a String property name
   * @param descriptor a {@link DatasetDescriptor}
   * @param defaultValue default value if prop is not present or is invalid
   * @return the value of prop parsed as an int or the default value
   */
  public static int getInt(String prop, DatasetDescriptor descriptor,
                            int defaultValue) {
    long asLong = getLong(prop, descriptor, defaultValue);
    if (asLong > Integer.MAX_VALUE) {
      return defaultValue;
    } else {
      return (int) asLong;
    }
  }
}
