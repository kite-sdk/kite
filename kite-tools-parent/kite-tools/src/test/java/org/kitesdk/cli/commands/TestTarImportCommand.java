/*
 * Copyright 2014 Cloudera Inc.
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

import com.beust.jcommander.internal.Lists;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.*;
import org.kitesdk.cli.TestUtil;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestTarImportCommand {

  private Logger console = null;
  private TarImportCommand command;

  private static String datasetUri;

  private static final int NUM_TEST_FILES = 10;
  private static final int MAX_FILENAME_LENGTH = 50;
  private static final int MAX_FILECONTENT_LENGTH = 5000;

  private static final String TEST_DATASET_NAME = "tarimport";
  private static final String TEST_DATASET_DIR = "target/datasets";
  private static final String TEST_DATA_DIR = "target/testdata";
  private static final String TAR_TEST_FILE = TEST_DATA_DIR + "/test.tar";
  private static final String TAR_TEST_ROOT_PREFIX = "testnone";
  private static final String TAR_TEST_GZIP_FILE =
      TEST_DATA_DIR + "/test.tar.gz";
  private static final String TAR_TEST_GZIP_ROOT_PREFIX = "testgzip";
  private static final String TAR_TEST_BZIP2_FILE =
      TEST_DATA_DIR + "/test.tar.bz2";
  private static final String TAR_TEST_BZIP2_ROOT_PREFIX = "testbzip2";
  private static final String TAR_TEST_LARGE_ENTRY_FILE =
      TEST_DATA_DIR + "/testlarge.tar";

  @BeforeClass
  public static void createTestInputFiles() throws IOException {
    TestTarImportCommand.cleanup();

    Path testData = new Path(TEST_DATA_DIR);
    FileSystem testFS = testData.getFileSystem(new Configuration());

    datasetUri = "dataset:file:" + System.getProperty(
        "user.dir") + "/" + TEST_DATASET_DIR + "/" + TEST_DATASET_NAME;

    TarArchiveOutputStream tosNoCompression = null;
    TarArchiveOutputStream tosGzipCompression = null;
    TarArchiveOutputStream tosBzip2Compression = null;
    TarArchiveOutputStream tosLargeEntry = null;
    TarArchiveEntry tarArchiveEntry = null;
    try {
      // No compression
      tosNoCompression =
          new TarArchiveOutputStream(testFS.create(new Path(TAR_TEST_FILE),
              true));
      writeToTarFile(tosNoCompression,
          TAR_TEST_ROOT_PREFIX + "/", null);

      // Gzip compression
      tosGzipCompression = new TarArchiveOutputStream(new
          GzipCompressorOutputStream(
          testFS.create(new Path(TAR_TEST_GZIP_FILE), true)));
      writeToTarFile(tosGzipCompression,
          TAR_TEST_GZIP_ROOT_PREFIX + "/", null);

      // BZip2 compression
      tosBzip2Compression = new TarArchiveOutputStream(new
          BZip2CompressorOutputStream(
          testFS.create(new Path(TAR_TEST_BZIP2_FILE), true)));
      writeToTarFile(tosBzip2Compression,
          TAR_TEST_BZIP2_ROOT_PREFIX + "/", null);

      // "Large" entry file (10000 bytes)
      tosLargeEntry = new TarArchiveOutputStream(
          testFS.create(new Path(TAR_TEST_LARGE_ENTRY_FILE), true));
      String largeEntry = RandomStringUtils.randomAscii(10000);
      writeToTarFile(tosLargeEntry, "largeEntry", largeEntry);

      // Generate test files with random names and content
      Random random = new Random(1);
      for (int i = 0; i < NUM_TEST_FILES; ++i) {
        // Create random file and data
        int fNameLength = random.nextInt(MAX_FILENAME_LENGTH);
        int fContentLength = random.nextInt(MAX_FILECONTENT_LENGTH);
        String fName = RandomStringUtils.randomAlphanumeric(fNameLength);
        String fContent = RandomStringUtils.randomAscii(fContentLength);

        // Write the file to tarball
        writeToTarFile(tosNoCompression,
            TAR_TEST_ROOT_PREFIX + "/" + fName, fContent);
        writeToTarFile(tosGzipCompression,
            TAR_TEST_GZIP_ROOT_PREFIX + "/" + fName, fContent);
        writeToTarFile(tosBzip2Compression,
            TAR_TEST_BZIP2_ROOT_PREFIX + "/" + fName, fContent);

        System.out.println("Wrote " + fName + " [" + fContentLength + "]");
      }
    } finally {
      IOUtils.closeStream(tosNoCompression);
      IOUtils.closeStream(tosGzipCompression);
      IOUtils.closeStream(tosBzip2Compression);
      IOUtils.closeStream(tosLargeEntry);
    }
  }

  @AfterClass
  public static void cleanupTests() throws IOException {
    TestTarImportCommand.cleanup();
  }

  private static void cleanup() throws IOException {
    FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    FileUtils.deleteDirectory(new File(TEST_DATASET_DIR));
  }

  private static void writeToTarFile(TarArchiveOutputStream tos, String name,
                                     String content) throws IOException {
    TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(name);
    if (null != content) {
      tarArchiveEntry.setSize(content.length());
    }
    tarArchiveEntry.setModTime(System.currentTimeMillis());
    tos.putArchiveEntry(tarArchiveEntry);
    if (null != content) {
      byte[] buf = content.getBytes();
      tos.write(buf, 0, content.length());
      tos.flush();
    }
    tos.closeArchiveEntry();
  }

  @Test
  public void testTarNoCompressionImportCreateDataset() throws Exception {
    removeData();
    command.targets = Lists.newArrayList(TAR_TEST_FILE, datasetUri);
    assertEquals(0, command.run());
    verify(console).info("Using {} compression",
        TarImportCommand.CompressionType.NONE);
    verify(console).info("Creating new dataset: {}", datasetUri);
    verify(console).info("Added {} records to \"{}\"",
        NUM_TEST_FILES,
        TEST_DATASET_NAME);
  }

  @Test
  public void testTarNoCompressionImportCommand() throws IOException {
    command.targets = Lists.newArrayList(TAR_TEST_FILE, datasetUri);
    command.compressionType = "none";
    assertEquals(0, command.run());
    verify(console).info("Using {} compression",
        TarImportCommand.CompressionType.NONE);
    verify(console).info("Added {} records to \"{}\"",
        NUM_TEST_FILES,
        TEST_DATASET_NAME);
  }

  @Test
  public void testTarNoCompressionImportCommandAutoDetect() throws IOException {
    command.targets = Lists.newArrayList(TAR_TEST_FILE, datasetUri);
    assertEquals(0, command.run());
    verify(console).info("Using {} compression",
        TarImportCommand.CompressionType.NONE);
    verify(console).info("Added {} records to \"{}\"",
        NUM_TEST_FILES,
        TEST_DATASET_NAME);
  }

  @Test
  public void testTarGzipImportCommand() throws IOException {
    command.targets = Lists.newArrayList(TAR_TEST_GZIP_FILE, datasetUri);
    command.compressionType = "gzip";
    assertEquals(0, command.run());
    verify(console).info("Using {} compression",
        TarImportCommand.CompressionType.GZIP);
    verify(console).info("Added {} records to \"{}\"",
        NUM_TEST_FILES,
        TEST_DATASET_NAME);
  }

  @Test
  public void testTarGzipImportCommandAutoDetect() throws IOException {
    command.targets = Lists.newArrayList(TAR_TEST_GZIP_FILE, datasetUri);
    assertEquals(0, command.run());
    verify(console).info("Using {} compression",
        TarImportCommand.CompressionType.GZIP);
    verify(console).info("Added {} records to \"{}\"",
        NUM_TEST_FILES,
        TEST_DATASET_NAME);
  }

  @Test
  public void testTarBzip2ImportCommand() throws IOException {
    command.targets =
        Lists.newArrayList(TAR_TEST_BZIP2_FILE, datasetUri);
    command.compressionType = "bzip2";
    assertEquals(0, command.run());
    verify(console).info("Using {} compression",
        TarImportCommand.CompressionType.BZIP2);
    verify(console).info("Added {} records to \"{}\"",
        NUM_TEST_FILES,
        TEST_DATASET_NAME);
  }

  @Test
  public void testTarBzip2ImportCommandAutoDetect() throws IOException {
    command.targets =
        Lists.newArrayList(TAR_TEST_BZIP2_FILE, datasetUri);
    assertEquals(0, command.run());
    verify(console).info("Using {} compression",
        TarImportCommand.CompressionType.BZIP2);
    verify(console).info("Added {} records to \"{}\"",
        NUM_TEST_FILES,
        TEST_DATASET_NAME);
  }

  @Test
  public void testWarnOnLargeEntry() throws IOException {
    command.getConf().setLong("dfs.blocksize",5000);
    command.targets =
        Lists.newArrayList(TAR_TEST_LARGE_ENTRY_FILE, datasetUri);
    assertEquals(0, command.run());
    verify(console).info("Using {} compression",
        TarImportCommand.CompressionType.NONE);
    verify(console).warn("Entry \"{}\" (size {}) is larger than the " +
            "HDFS block size of {}. This may result in remote block reads",
        new Object[] { "largeEntry", 10000l, 5000l });
    verify(console).info("Added {} records to \"{}\"",
        1,
        TEST_DATASET_NAME);
  }

  @Before
  public void setup() throws Exception {
    TestUtil.run("-v", "create", datasetUri, "-s",
        "kite-tools-parent/kite-tools/src/main/avro/tar-import.avsc");
    this.console = mock(Logger.class);
    this.command = new TarImportCommand(console);
    command.setConf(new Configuration());
    command.directory = TEST_DATASET_DIR;
    command.local = true;
  }

  @After
  public void removeData() throws Exception {
    TestUtil.run("delete", TEST_DATASET_NAME, "--use-local", "-d",
        TEST_DATASET_DIR);
  }

}
