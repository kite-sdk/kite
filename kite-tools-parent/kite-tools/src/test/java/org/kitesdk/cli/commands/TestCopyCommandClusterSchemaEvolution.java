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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.kitesdk.cli.TestUtil;
import com.google.common.collect.Lists;
import static org.mockito.Mockito.mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestCopyCommandClusterSchemaEvolution extends TestCopyCommandCluster {

  private static final String evolvedAvsc = "target/evolved_user.avsc";

  @Override
  public void createDestination() throws Exception {
    FileInputStream schemaIn = new FileInputStream(avsc);
    Schema original = new Schema.Parser().parse(schemaIn);
    schemaIn.close();

    Schema evolved = getEvolvedSchema(original);

    FileOutputStream schemaOut = new FileOutputStream(evolvedAvsc);
    schemaOut.write(evolved.toString(true).getBytes());
    schemaOut.close();

    List<String> createArgs = Lists.newArrayList(
      "create", dest, "-s", evolvedAvsc, "-r", repoUri, "-d", "target/data");
    createArgs.addAll(getExtraCreateArgs());

    TestUtil.run(LoggerFactory.getLogger(this.getClass()),
      "delete", dest, "-r", repoUri, "-d", "target/data");
    TestUtil.run(LoggerFactory.getLogger(this.getClass()),
      createArgs.toArray(new String[createArgs.size()]));
    this.console = mock(Logger.class);
    this.command = new CopyCommand(console);
    command.setConf(new Configuration());
  }

  public abstract Schema getEvolvedSchema(Schema original) throws Exception;

  public abstract List<String> getExtraCreateArgs() throws Exception;
  
}
