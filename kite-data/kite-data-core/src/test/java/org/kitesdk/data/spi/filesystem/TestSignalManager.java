/**
 * Copyright 2015 Cloudera Inc.
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
package org.kitesdk.data.spi.filesystem;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.Constraints;
import com.google.common.io.Files;

@RunWith(Parameterized.class)
public class TestSignalManager extends MiniDFSTest {

  Path testDirectory;
  Configuration conf;
  FileSystem fileSystem;

  boolean distributed;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
            { false },  // default to local FS
            { true } }; // default to distributed FS
    return Arrays.asList(data);
  }

  public TestSignalManager(boolean distributed) {
    this.distributed = distributed;
  }

  @Before
  public void setup() throws IOException {
    this.conf = (distributed ?
            MiniDFSTest.getConfiguration() :
            new Configuration());

    this.fileSystem = FileSystem.get(conf);

    Path baseDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
    this.testDirectory = new Path(baseDirectory, ".signals");
  }

  @Test
  public void testNormalizeConstraints() throws IOException {
    Constraints constraints = new Constraints(DatasetTestUtilities.USER_SCHEMA).
        with("email", "user@domain.com");

    String normalizedConstraints = SignalManager.getNormalizedConstraints(constraints);

    Assert.assertEquals("email=user%40domain.com", normalizedConstraints);
  }

  @Test
  public void testNormalizeConstraintsUnbounded() throws IOException {
    Constraints constraints = new Constraints(DatasetTestUtilities.USER_SCHEMA);

    String normalizedConstraints = SignalManager.getNormalizedConstraints(constraints);

    Assert.assertEquals("unbounded", normalizedConstraints);
  }

  @Test
  public void testNormalizeConstraintsValueExists() throws IOException {
    Constraints constraints = new Constraints(DatasetTestUtilities.USER_SCHEMA).
        with("email", "");

    String normalizedConstraints = SignalManager.getNormalizedConstraints(constraints);

    Assert.assertEquals("email=in()", normalizedConstraints);
  }

  @Test
  public void testNormalizeConstraintsOrderedSets() throws IOException {
    Constraints constraints = new Constraints(DatasetTestUtilities.OLD_VALUE_SCHEMA).
        with("value", 7L,2L,3L);

    String normalizedConstraints = SignalManager.getNormalizedConstraints(constraints);

    Assert.assertEquals("value=2,3,7", normalizedConstraints);
  }

  @Test
  public void testNormalizeConstraintsIntervals() throws IOException {
    Constraints constraints = new Constraints(DatasetTestUtilities.OLD_VALUE_SCHEMA).
        toBefore("value", 12L);

    String normalizedConstraints = SignalManager.getNormalizedConstraints(constraints);

    Assert.assertEquals("value=(,12)", normalizedConstraints);
  }

  @Test
  public void testNormalizeConstraintsOrderedKeys() throws IOException {
    Constraints constraints = new Constraints(DatasetTestUtilities.USER_SCHEMA).
        with("username", "kite").with("email", "kite@domain.com");

    String normalizedConstraints = SignalManager.getNormalizedConstraints(constraints);

    Assert.assertEquals("email=kite%40domain.com&username=kite", normalizedConstraints);
  }

  @Test
  public void testSignalDirectoryCreatedOnSignal() throws IOException {
    SignalManager manager = new SignalManager(fileSystem, testDirectory);

    Assert.assertFalse("Signal directory shouldn't exist before signals",
        fileSystem.exists(testDirectory));

    Constraints constraints = new Constraints(DatasetTestUtilities.USER_SCHEMA).
        with("email", "signalCreatesDir@domain.com");
    manager.signalReady(constraints);

    Assert.assertTrue("Signal directory created on signals",
        fileSystem.exists(testDirectory));
  }

  @Test
  public void testConstraintsSignaledReady() throws IOException {
    SignalManager manager = new SignalManager(fileSystem, testDirectory);

    Constraints constraints = new Constraints(DatasetTestUtilities.USER_SCHEMA).
        with("email", "testConstraintsSignaledReady@domain.com");
    manager.signalReady(constraints);

    String normalizedConstraints = SignalManager.getNormalizedConstraints(constraints);
    Assert.assertTrue(this.fileSystem.exists(new Path(this.testDirectory,
        normalizedConstraints)));
  }

  @Test
  public void testMultiConstraintsSignaledReady() throws IOException {
    SignalManager manager = new SignalManager(fileSystem, testDirectory);

    Constraints constraints = new Constraints(DatasetTestUtilities.USER_SCHEMA).
        with("email", "kiteuser@domain.com").with("username", "kiteuser");
    manager.signalReady(constraints);

    String normalizedConstraints = SignalManager.getNormalizedConstraints(constraints);
    Assert.assertTrue(this.fileSystem.exists(new Path(this.testDirectory,
        normalizedConstraints)));
  }

  @Test
  public void testConstraintsSignaledReadyPreviouslySignaled()
      throws IOException, InterruptedException {
    SignalManager manager = new SignalManager(fileSystem, testDirectory);

    Constraints constraints =
        new Constraints(DatasetTestUtilities.USER_SCHEMA)
          .with("email",
              "testConstraintsSignaledReadyPreviouslySignaled@domain.com");

    String normalizedConstraints = SignalManager.getNormalizedConstraints(constraints);
    Path signalFilePath = new Path(this.testDirectory,normalizedConstraints);

    manager.signalReady(constraints);

    Assert.assertTrue(this.fileSystem.exists(signalFilePath));
    long firstSignalContents = this.fileSystem.getFileStatus(signalFilePath).getModificationTime();

    // the modified time depends on the filesystem, and may only be granular to the second
    // signal and check until the modified time is after the current time, or until
    // enough time has past that the signal should have been distinguishable
    long spinStart = System.currentTimeMillis();
    long signaledTime = 0;
    while(firstSignalContents >= signaledTime && (System.currentTimeMillis() - spinStart) <= 2000) {
      manager.signalReady(constraints);
      signaledTime = manager.getReadyTimestamp(constraints);
    }

    Assert.assertFalse("Second signal should not match the first",
        signaledTime == firstSignalContents);
  }

  @Test
  public void testConstraintsGetReadyTimestamp() throws IOException {
    SignalManager manager = new SignalManager(fileSystem, testDirectory);

    Constraints constraints = new Constraints(DatasetTestUtilities.USER_SCHEMA).
        with("email", "testConstraintsReady@domain.com");

    Path signalFilePath = new Path(this.testDirectory,
        "email=testConstraintsReady%40domain.com");
    // drop a file at the signal path
    FSDataOutputStream stream = this.fileSystem.create(signalFilePath, true);
    stream.writeUTF(String.valueOf(System.currentTimeMillis()));
    stream.close();

    Assert.assertTrue(manager.getReadyTimestamp(constraints) != -1);
  }

  @Test
  public void testConstraintsGetReadyTimestampNotYetSignaled() throws IOException {
    SignalManager manager = new SignalManager(fileSystem, testDirectory);

    Constraints constraints = new Constraints(DatasetTestUtilities.USER_SCHEMA).
        with("email", "testConstraintsGetReadyTimestampNotYetSignaled@domain.com");

    Assert.assertEquals("A constraint that is not signaled should show -1",
        -1, manager.getReadyTimestamp(constraints));
  }

}
