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

import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;

public class SchemaUtil {

  public static final ImmutableMap<Schema.Type, Class<?>> TYPE_TO_CLASS =
      ImmutableMap.<Schema.Type, Class<?>>builder()
          .put(Schema.Type.BOOLEAN, Boolean.class)
          .put(Schema.Type.INT, Integer.class)
          .put(Schema.Type.LONG, Long.class)
          .put(Schema.Type.FLOAT, Float.class)
          .put(Schema.Type.DOUBLE, Double.class)
          .put(Schema.Type.STRING, String.class)
          .put(Schema.Type.BYTES, ByteBuffer.class)
          .build();

  public static boolean checkType(Schema.Type type, Class<?> expectedClass) {
    Class<?> typeClass = TYPE_TO_CLASS.get(type);
    return typeClass != null && expectedClass.isAssignableFrom(typeClass);
  }

}
