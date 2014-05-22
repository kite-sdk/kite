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
package org.kitesdk.cli.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.URIPattern;
import org.slf4j.Logger;

import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestCreateDatasetCommandCluster extends MiniDFSTest {

  private static final AtomicInteger ids = new AtomicInteger(0);
  private static final Map<String, DatasetRepository> repos = Maps.newHashMap();
  private String id = null;
  private CreateDatasetCommand command = null;
  private Logger console;

  @BeforeClass
  public static void addMockRepoBuilder() throws Exception {
    org.kitesdk.data.impl.Accessor.getDefault().registerDatasetRepository(
        new URIPattern("mock::id"), new OptionBuilder<DatasetRepository>() {
          @Override
          public DatasetRepository getFromOptions(Map<String, String> options) {
            DatasetRepository repo = mock(DatasetRepository.class);
            repos.put(options.get("id"), repo);
            return repo;
          }
        });
  }

  @Before
  public void setUp() {
    this.id = Integer.toString(ids.addAndGet(1));
    this.console = mock(Logger.class);
    this.command = new CreateDatasetCommand(console);
    this.command.setConf(getConfiguration());
    this.command.repoURI = "repo:mock:" + id;
  }

  public DatasetRepository getMockRepo() {
    return repos.get(id);
  }

  @Test
  public void testBasicUse() throws Exception {
    command.avroSchemaFile = "test-schemas/user.avsc";
    command.datasetNames = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:test-schemas/user.avsc")
        .build();

    verify(getMockRepo()).create("users", expectedDescriptor);
    verify(console).debug(contains("Created"), eq("users"));
  }

  @Test
  public void testBasicUseLocalSchema() throws Exception {
    String avsc = "target/localUser.avsc";
    FSDataOutputStream out = getFS()
        .create(new Path(avsc), true /* overwrite */ );
    ByteStreams.copy(Resources.getResource("test-schemas/user.avsc").openStream(), out);
    out.close();
    command.avroSchemaFile = avsc;
    command.datasetNames = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:test-schemas/user.avsc")
        .build();

    verify(getMockRepo()).create("users", expectedDescriptor);
    verify(console).debug(contains("Created"), eq("users"));
  }

  @Test
  public void testBasicUseHDFSSchema() throws Exception {
    String avsc = "hdfs:/tmp/schemas/hdfsUser.avsc";
    FSDataOutputStream out = getDFS()
        .create(new Path(avsc), true /* overwrite */ );
    ByteStreams.copy(Resources.getResource("test-schemas/user.avsc").openStream(), out);
    out.close();
    command.avroSchemaFile = avsc;
    command.datasetNames = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:test-schemas/user.avsc")
        .build();

    verify(getMockRepo()).create("users", expectedDescriptor);
    verify(console).debug(contains("Created"), eq("users"));
  }

}
