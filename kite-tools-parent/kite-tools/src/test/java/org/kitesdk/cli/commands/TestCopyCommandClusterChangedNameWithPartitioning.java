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
import java.io.FileOutputStream;
import java.util.List;
import org.apache.avro.Schema;
import org.kitesdk.data.PartitionStrategy;

public class TestCopyCommandClusterChangedNameWithPartitioning extends TestCopyCommandClusterSchemaEvolution {

  private static final String partitionStrategyJson = "target/partition-strategy.json";

  @Override
  public Schema getEvolvedSchema(Schema original) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : original.getFields()) {
      fields.add(new Schema.Field(field.name(), field.schema(), field.doc(),
        field.defaultValue()));
    }

    Schema evolved = Schema.createRecord("NewUser", original.getDoc(),
      original.getNamespace(), false);
    evolved.addAlias("User");
    evolved.setFields(fields);

    return evolved;
  }

  @Override
  public List<String> getExtraCreateArgs() throws Exception {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("id", "part", 5)
      .build();

    FileOutputStream psOut = new FileOutputStream(partitionStrategyJson);
    psOut.write(partitionStrategy.toString(true).getBytes());
    psOut.close();

    return ImmutableList.of(
        "-p", partitionStrategyJson,
        "--set", "kite.writer.cache-size=20");
  }

  @Override
  public void testCopyWithoutCompaction() throws Exception {
    testCopyWithoutCompaction(5);
  }

  @Override
  public void testCopyWithNumWriters() throws Exception {
    testCopyWithNumWriters(5);
  }
  
}
