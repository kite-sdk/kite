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

package org.kitesdk.compat;

import com.google.common.collect.Lists;
import java.util.List;

public class DynClasses {

  public static class Builder {
    private final List<String> candidates = Lists.newArrayList();
    private ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private boolean defaultNull = true;
    private Class<?> targetClass;

    /**
     * Set the {@link ClassLoader} used to lookup classes by name.
     * <p>
     * If not set, the current thread's ClassLoader is used.
     *
     * @param loader a ClassLoader
     * @return this Builder for method chaining
     */
    public Builder loader(ClassLoader loader) {
      this.loader = loader;
      return this;
    }

    /**
     * Checks for the given class by name.
     *
     * @param className name of a class
     * @return this Builder for method chaining
     * @see {@link Class#forName(String)}
     */
    public Builder impl(String className) {
      // don't do any work if an implementation has been found
      if (targetClass != null) {
        return this;
      }

      try {
        candidates.add(className);
        targetClass = Class.forName(className, true, loader);
      } catch (ClassNotFoundException e) {
        // not the right implementation
      }
      return this;
    }

    /**
     * Return null from build methods if no candidate class is found.
     *
     * @return this Builder for method chaining.
     */
    public Builder defaultNull() {
      this.defaultNull = true;
      return this;
    }

    /**
     * Returns the first valid implementation class.
     *
     * @return a {@link Class} with a valid implementation
     * @throws NoSuchMethodException if no implementation was found
     */
    @SuppressWarnings("unchecked")
    public <T> Class<T> buildChecked() throws ClassNotFoundException {
      if (targetClass != null || defaultNull) {
        return (Class<T>) targetClass;
      } else {
        throw new ClassNotFoundException("Cannot find any class in: " + candidates);
      }
    }

    /**
     * Returns the first valid implementation class.
     *
     * @return a {@link Class} with a valid implementation
     * @throws RuntimeException if no implementation was found
     */
    @SuppressWarnings("unchecked")
    public <T> Class<T> build() {
      if (targetClass != null || defaultNull) {
        return (Class<T>) targetClass;
      } else {
        throw new RuntimeException("Cannot find any class in: " + candidates);
      }
    }
  }
}
