/*
 * Copyright 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.cli.commands;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;

public class TestCopyCommandClusterNewField extends TestCopyCommandClusterSchemaEvolution {

  @Override
  public Schema getEvolvedSchema(Schema original) {
    List<Schema.Field> fields = Lists.newArrayList();
    fields.add(new Schema.Field("new",
      Schema.createUnion(ImmutableList.of(
          Schema.create(Schema.Type.NULL),
          Schema.create(Schema.Type.STRING))),
      "New field", NullNode.getInstance()));

    for (Schema.Field field : original.getFields()) {
      fields.add(new Schema.Field(field.name(), field.schema(), field.doc(),
        field.defaultValue()));
    }

    Schema evolved = Schema.createRecord(original.getName(), original.getDoc(),
      original.getNamespace(), false);
    evolved.setFields(fields);

    return evolved;
  }

  @Override
  public List<String> getExtraCreateArgs() {
    return ImmutableList.of();
  }

}
