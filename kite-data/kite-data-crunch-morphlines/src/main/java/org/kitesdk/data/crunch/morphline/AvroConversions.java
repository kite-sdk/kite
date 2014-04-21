/*
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
package org.kitesdk.data.crunch.morphline;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.kitesdk.morphline.api.MorphlineRuntimeException;


/**
 * Reusable utilities for converting morphline records to Avro.
 */
final class AvroConversions {
  
  private AvroConversions() {}
    
  // more efficient than raising & catching exceptions
  static final Object ERROR = new Object(); 

  /* returns true if schema allows the value to be null, false otherwise */
  private static boolean nullOk(Schema schema) {
    if (Schema.Type.NULL == schema.getType()) {
      return true;
    } else if (Schema.Type.UNION == schema.getType()) {
      for (Schema candidate : schema.getTypes()) {
        if (nullOk(candidate)) {
          return true;
        }
      }
    }
    return false;
  }
  
  static Object toAvro(Object item, Field field) {
    if (item == null && !nullOk(field.schema())) {
      try { // this will fail if there is no default value
        return ReflectData.get().getDefaultValue(field);
      } catch (AvroRuntimeException e) {
        return ERROR;
      }
    }
    Object result = toAvro(item, field.schema());
    return result;
  }
  
  static Object toAvro(Object item, Schema schema) {
    // RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT,
    // DOUBLE, BOOLEAN, NULL
    switch (schema.getType()) {
      case RECORD:
        if (item instanceof Map) {
          Map<String,Object> map = (Map) item;
          IndexedRecord record = new GenericData.Record(schema);
          for (Field field : schema.getFields()) {
            Object value = map.get(field.name());
            Object result = toAvro(value, field);
            if (result == ERROR) {
              return ERROR;
            }
            record.put(field.pos(), result);
          }
          return record;
        }
        return ERROR;
      case ENUM:
        if (schema.hasEnumSymbol(item.toString())) {
          return item.toString();
        } 
        return ERROR;
      case ARRAY:
        if (item instanceof List) {
          ListIterator iter = ((List)item).listIterator();
          while (iter.hasNext()) {
            Object result = toAvro(iter.next(), schema.getElementType());
            if (result == ERROR) {
              return ERROR;
            }
            iter.set(result);
          }
          return item;
        }
        return ERROR;
      case MAP:
        if (item instanceof Map) {
          Map<String,Object> map = (Map) item;
          for (Map.Entry entry : map.entrySet()) {
            if (!(entry.getKey() instanceof CharSequence)) {
              return ERROR; // Avro requires that map keys are CharSequences 
            }
            Object result = toAvro(entry.getValue(), schema.getValueType());
            if (result == ERROR) {
              return ERROR;
            }
            entry.setValue(result);
          }
          return item;
        }
        return ERROR;
      case UNION:
        return toAvroUnion(item, schema);
      case FIXED:
        if (item instanceof byte[]) {
          return new GenericData.Fixed(schema, (byte[])item);
        }          
        return ERROR;
      case STRING:
        assert item != null;
        return item.toString();
      case BYTES:
        if (item instanceof ByteBuffer) {
          return item;
        }
        if (item instanceof byte[]) {
          return ByteBuffer.wrap((byte[])item);
        }  
        return ERROR;
      case INT:
        if (item instanceof Integer) {
          return item;
        }
        if (item instanceof Number) {
          return ((Number) item).intValue();
        }
        try {
          return Integer.valueOf(item.toString());
        } catch (NumberFormatException e) {
          return ERROR;
        }
      case LONG:
        if (item instanceof Long) {
          return item;
        }
        if (item instanceof Number) {
          return ((Number) item).longValue();
        }
        try {
          return Long.valueOf(item.toString());
        } catch (NumberFormatException e) {
          return ERROR;
        }
      case FLOAT:
        if (item instanceof Float) {
          return item;
        }
        if (item instanceof Number) {
          return ((Number) item).floatValue();
        }
        try {
          return Float.valueOf(item.toString());
        } catch (NumberFormatException e) {
          return ERROR;
        }
      case DOUBLE:
        if (item instanceof Double) {
          return item;
        }
        if (item instanceof Number) {
          return ((Number) item).doubleValue();
        }
        try {
          return Double.valueOf(item.toString());
        } catch (NumberFormatException e) {
          return ERROR;
        }
      case BOOLEAN:
        if (item instanceof Boolean) {
          return item;
        }
        assert item != null;
        String str = item.toString();
        if ("true".equals(str)) {
          return Boolean.TRUE;
        }
        if ("false".equals(str)) {
          return Boolean.FALSE;
        }
        return ERROR;
      case NULL:
        if (item == null) {
          return null;
        }
        return ERROR;
      default:
        throw new MorphlineRuntimeException("Unknown Avro schema type: " + schema.getType());
    }
  }

  private static Object toAvroUnion(Object item, Schema schema) {
    assert schema.getType() == Schema.Type.UNION;
    List<Schema> types = schema.getTypes();
    int index = -1;
    if (item instanceof Map) {
      // a map can be converted both into an avro record or an avro map.
      // so there's some ambiguity - we choose which one applies based on specified order.
      for (int j = 0; j < types.size(); j++) {
        Schema.Type t = types.get(j).getType(); 
        if (t == Schema.Type.RECORD || t == Schema.Type.MAP) {
          index = j;
          break;
        }
      }
    } else {
      try {
        // check if there's a perfect fit for a mapping
        index = GenericData.get().resolveUnion(schema, item); // TODO: optimize
      } catch (AvroRuntimeException e) {
        ; // proceed to find first fit based on specified order (see below)
        // LOG.trace("Cannot find perfect fit for item: {} to union schema: {}", item, schema);
      }
    }
    
    if (index >= 0) { // found perfect fit
      Schema candidate = types.get(index);
      Object result = toAvro(item, candidate);
      return result;
    } else { // find first fit based on specified order
      for (Schema candidate : types) {            
        Object result = toAvro(item, candidate);
        if (result != ERROR) {
          return result;
        }
      }
      return ERROR;
    }
  }
    
}
