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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.partition.ProvidedFieldPartitioner;

@Immutable
public class EntityAccessor<E> {

  private final Schema schema;
  private final Schema writeSchema;
  private final Class<E> type;
  private final GenericData model;
  private final Map<String, List<Schema.Field>> cache = Maps.newHashMap();

  EntityAccessor(Class<E> type, Schema schema) {
    this.type = DataModelUtil.resolveType(type, schema);
    this.schema = DataModelUtil.getReaderSchema(this.type, schema);
    this.writeSchema = DataModelUtil.getWriterSchema(this.type, this.schema);
    this.model = DataModelUtil.getDataModelForType(this.type);
  }

  public Class<E> getType() {
    return type;
  }

  public Schema getReadSchema() {
    return schema;
  }

  public Schema getWriteSchema() {
    return writeSchema;
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
        // assume that the nested schemas are Records or nullable Records
        // this is checked by SchemaUtil.fieldSchema(Schema, String)
        if (nested.getType() == Schema.Type.UNION) {
          // nullable Records are not allowed in partition fields, but the read
          // schema may contain nullable records when using reflection.
          List<Schema> types = nested.getTypes();
          if (types.get(0).getType() == Schema.Type.NULL) {
            nested = types.get(1);
          } else {
            nested = types.get(0);
          }
        }
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

  public StorageKey keyFor(E object, @Nullable Map<String, Object> provided,
                           StorageKey reuse) {
    Preconditions.checkNotNull(reuse, "Cannot use null key");
    PartitionStrategy strategy = reuse.getPartitionStrategy();
    List<FieldPartitioner> partitioners =
        Accessor.getDefault().getFieldPartitioners(strategy);
    for (int i = 0, n = partitioners.size(); i < n; i += 1) {
      reuse.replace(i, partitionValue(object, provided, partitioners.get(i)));
    }
    return reuse;
  }

  @VisibleForTesting
  StorageKey keyFor(E object, StorageKey reuse) {
    return keyFor(object, null, reuse);
  }

  @SuppressWarnings("unchecked")
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
      justification="Null case checked by precondition")
  private Object partitionValue(E object, @Nullable Map<String, Object> provided,
                                FieldPartitioner fp) {
    if (fp instanceof ProvidedFieldPartitioner) {
      String name = fp.getName();
      Preconditions.checkArgument(
          (provided != null) && provided.containsKey(name),
          "Cannot construct key, missing provided value: %s", name);
      return provided.get(name);
    } else {
      return fp.apply(get(object, fp.getSourceName()));
    }
  }

}
