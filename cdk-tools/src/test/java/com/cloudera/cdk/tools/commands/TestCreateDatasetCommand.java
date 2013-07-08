/**
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
package com.cloudera.cdk.tools.commands;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.Formats;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.cloudera.cdk.data.hcatalog.HCatalogDatasetRepository;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCreateDatasetCommand {

  private FileSystemDatasetRepository fsRepo;
  private HCatalogDatasetRepository hcatRepo;

  private CreateDatasetCommand command;

  @Before
  public void setUp() throws Exception {
    FileSystemDatasetRepository.Builder fsRepoBuilder = mock
        (FileSystemDatasetRepository.Builder.class);
    fsRepo = mock(FileSystemDatasetRepository.class);
    when(fsRepoBuilder.rootDirectory((URI) anyObject())).thenReturn(fsRepoBuilder);
    when(fsRepoBuilder.configuration((Configuration) anyObject())).thenReturn(fsRepoBuilder);
    when(fsRepoBuilder.get()).thenReturn(fsRepo);

    HCatalogDatasetRepository.Builder hcatRepoBuilder = mock(HCatalogDatasetRepository
        .Builder.class);
    hcatRepo = mock(HCatalogDatasetRepository.class);
    when(hcatRepoBuilder.rootDirectory((URI) anyObject())).thenReturn(hcatRepoBuilder);
    when(hcatRepoBuilder.configuration((Configuration) anyObject())).thenReturn(hcatRepoBuilder);
    when(hcatRepoBuilder.get()).thenReturn(hcatRepo);

    command = new CreateDatasetCommand();
    command.fsRepoBuilder = fsRepoBuilder;
    command.hcatRepoBuilder = hcatRepoBuilder;
    command.setConf(new Configuration());
  }

  @Test
  public void testDefaults() throws Exception {
    command.avroSchemaFile = "user.avsc";
    command.datasetNames = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schema(Resources.getResource("user.avsc").openStream()).get();

    verify(hcatRepo).create("users", expectedDescriptor);
  }

  @Test
  public void testFileSystemMetadataProvider() throws Exception {
    command.hcatalog = false;
    command.directory = "/tmp/data";
    command.avroSchemaFile = "user.avsc";
    command.datasetNames = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schema(Resources.getResource("user.avsc").openStream()).get();

    verify(fsRepo).create("users", expectedDescriptor);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDirectoryRequiredWithFileSystemMetadataProvider() throws Exception {
    command.hcatalog = false;
    command.avroSchemaFile = "user.avsc";
    command.datasetNames = Lists.newArrayList("users");
    command.run();
  }

  @Test
  public void testParquet() throws Exception {
    command.avroSchemaFile = "user.avsc";
    command.format = Formats.PARQUET.getName();
    command.datasetNames = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schema(Resources.getResource("user.avsc").openStream())
        .format(Formats.PARQUET).get();

    verify(hcatRepo).create("users", expectedDescriptor);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnrecognizedFormat() throws Exception {
    command.avroSchemaFile = "user.avsc";
    command.format = "nosuchformat";
    command.datasetNames = Lists.newArrayList("users");
    command.run();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonExistentAvroSchemaFile() throws Exception {
    command.avroSchemaFile = "nonexistent.avsc";
    command.datasetNames = Lists.newArrayList("users");
    command.run();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultipleDatasets() throws Exception {
    command.avroSchemaFile = "user.avsc";
    command.datasetNames = Lists.newArrayList("users", "moreusers");
    command.run();
  }

  @Test
  public void testPartitionExpression() throws Exception {
    command.avroSchemaFile = "user.avsc";
    command.partitionExpression =
        "[year(\"timestamp\", \"year\"), month(\"timestamp\", \"month\")]";
    command.datasetNames = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schema(Resources.getResource("user.avsc").openStream())
        .partitionStrategy(new PartitionStrategy.Builder().year("timestamp",
            "year").month("timestamp", "month").get()).get();

    verify(hcatRepo).create("users", expectedDescriptor);
  }
}
