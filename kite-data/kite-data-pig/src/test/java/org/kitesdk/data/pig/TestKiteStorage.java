/*
 * Copyright 2014 Cloudera, Inc.
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
package org.kitesdk.data.pig;

import java.io.File;
import java.io.IOException;
import org.apache.avro.generic.GenericData;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.USER_SCHEMA;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.checkTestUsers;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.writeTestUsers;

public class TestKiteStorage {

  private static PigServer pigServerLocal = null;
  private static String outbasedir;
  private static final String TMP_DIR = System.getProperty("buildDirectory") + "/tmp/pig";

  // Adapted from Apache Pig (contrib/piggybank/java/src/test/java/org/apache/pig/piggybank/test/storage/avro/TestAvroStorage.java)
  @BeforeClass
  public static void setup() throws ExecException, IOException {
    pigServerLocal = new PigServer(ExecType.LOCAL);
    pigServerLocal.getPigContext().getProperties().setProperty("pig.temp.dir", TMP_DIR);
    outbasedir = FileLocalizer.getTemporaryPath(pigServerLocal.getPigContext()).toString() + "/TestKiteStorage/";
    deleteDirectory(new File(outbasedir));
  }

  // Adapted from Apache Pig (contrib/piggybank/java/src/test/java/org/apache/pig/piggybank/test/storage/avro/TestAvroStorage.java)
  @AfterClass
  public static void teardown() {
    if (pigServerLocal != null) {
      pigServerLocal.shutdown();
    }
  }

  @Test
  public void testGeneric() throws IOException {
    String inUri = createUri("in");
    if (Datasets.exists(inUri)) {
      Datasets.delete(inUri);
    }
    Dataset<GenericData.Record> inputDataset = Datasets.create(inUri,
        new DatasetDescriptor.Builder().schema(USER_SCHEMA).build(),
        GenericData.Record.class);

    String outUri = createUri("out");
    if (Datasets.exists(outUri)) {
      Datasets.delete(outUri);
    }
    Dataset<GenericData.Record> outputDataset = Datasets.create(outUri,
        new DatasetDescriptor.Builder().schema(USER_SCHEMA).build(),
        GenericData.Record.class);

    // write two files, each of 5 records
    writeTestUsers(inputDataset, 5, 0);
    writeTestUsers(inputDataset, 5, 5);

    String[] queries = {
      " in = LOAD '" + inUri
      + "' USING org.kitesdk.data.pig.KiteStorage;",
      " STORE in INTO '" + outUri
      + "' USING org.kitesdk.data.pig.KiteStorage;"
    };
    TestKiteStorage.this.testKiteStorage(queries);

    checkTestUsers(outputDataset, 10);
  }

  @Ignore("Fails due to double creating of the temp repository by DatasetKeyOutputFormat")
  @Test
  public void testMultipleOutputs() throws IOException {
    String inUri = createUri("in");
    if (Datasets.exists(inUri)) {
      Datasets.delete(inUri);
    }
    Dataset<GenericData.Record> inputDataset = Datasets.create(inUri,
        new DatasetDescriptor.Builder().schema(USER_SCHEMA).build(),
        GenericData.Record.class);

    String outUri1 = createUri("out-1");
    if (Datasets.exists(outUri1)) {
      Datasets.delete(outUri1);
    }
    Dataset<GenericData.Record> outputDataset1 = Datasets.create(outUri1,
        new DatasetDescriptor.Builder().schema(USER_SCHEMA).build(),
        GenericData.Record.class);

    String outUri2 = createUri("out-2");
    if (Datasets.exists(outUri2)) {
      Datasets.delete(outUri2);
    }
    Dataset<GenericData.Record> outputDataset2 = Datasets.create(outUri2,
        new DatasetDescriptor.Builder().schema(USER_SCHEMA).build(),
        GenericData.Record.class);

    // write two files, each of 5 records
    writeTestUsers(inputDataset, 5, 0);
    writeTestUsers(inputDataset, 5, 5);

    String[] queries = {
      " in = LOAD '" + inUri
      + "' USING org.kitesdk.data.pig.KiteStorage;",
      " STORE in INTO '" + outUri1
      + "' USING org.kitesdk.data.pig.KiteStorage;",
      " STORE in INTO '" + outUri2
      + "' USING org.kitesdk.data.pig.KiteStorage;"
    };
    TestKiteStorage.this.testKiteStorage(queries);

    checkTestUsers(outputDataset1, 10);
    checkTestUsers(outputDataset2, 10);
  }

  // Adapted from Apache Pig (contrib/piggybank/java/src/test/java/org/apache/pig/piggybank/test/storage/avro/TestAvroStorage.java)
  private void testKiteStorage(String... queries) throws IOException {
    testKiteStorage(false, queries);
  }

  // Adapted from Apache Pig (contrib/piggybank/java/src/test/java/org/apache/pig/piggybank/test/storage/avro/TestAvroStorage.java)
  private void testKiteStorage(boolean expectedToFail, String... queries) throws IOException {
    pigServerLocal.setBatchOn();
    for (String query : queries) {
      if (query != null && query.length() > 0) {
        pigServerLocal.registerQuery(query);
      }
    }
    int numOfFailedJobs = 0;
    for (ExecJob job : pigServerLocal.executeBatch()) {
      if (job.getStatus().equals(JOB_STATUS.FAILED)) {
        numOfFailedJobs++;
      }
    }
    if (expectedToFail) {
      assertTrue("There was no failed job!", numOfFailedJobs > 0);
    } else {
      assertTrue("There was a failed job!", numOfFailedJobs == 0);
    }
  }

  // Adapted from Apache Pig (contrib/piggybank/java/src/test/java/org/apache/pig/piggybank/test/storage/avro/TestAvroStorage.java)
  private static void deleteDirectory(File path) {
    if (path.exists()) {
      File[] files = path.listFiles();
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        }
        file.delete();
      }
    }
  }

  private String createUri(String dataset) {
    return "dataset:file:" + TMP_DIR + "/" + dataset;
  }

}
