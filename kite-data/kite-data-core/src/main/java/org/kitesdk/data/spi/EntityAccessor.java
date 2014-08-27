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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

@Immutable
public class EntityAccessor<E> {

  private final Schema schema;
  private final Class<E> type;
  private final GenericData model;
  private final Map<String, List<Schema.Field>> cache = Maps.newHashMap();

  EntityAccessor(Class<E> type, Schema schema) {
    this.type = DataModelUtil.resolveType(type, schema);
    this.schema = DataModelUtil.getReaderSchema(this.type, schema);
    this.model = DataModelUtil.getDataModelForType(this.type);
  }

  public Class<E> getType() {
    return type;
  }

  public Schema getEntitySchema() {
    return schema;
  }

  public Object get(E object, String name) {
    List<Schema.Field> fields = cache.get(name);

    Object value;
    if (fields != null) {
      value = get(object, fields);

    } else {
      value = object;
      fields = Lists.newArrayList();
      Schema nested = schema;
      for (String level : SchemaUtil.NAME_SPLITTER.split(name)) {
        // assume that the nested schemas are Records
        // this is checked by SchemaUtil.fieldSchema(Schema, String)
        Schema.Field field = nested.getField(level);
        fields.add(field);
        nested = field.schema();
        value = model.getField(value, level, field.pos());
      }
      cache.put(name, fields);
    }

    return value;
  }

  public Object get(E object, Iterable<Schema.Field> fields) {
    Object value = object;
    for (Schema.Field level : fields) {
      value = model.getField(value, level.name(), level.pos());
    }
    return value;
  }
}
