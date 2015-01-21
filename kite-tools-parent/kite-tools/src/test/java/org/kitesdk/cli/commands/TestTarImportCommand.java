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
import org.apache.commons.compress.compressors.bzip2.
    BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.*;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarHeader;
import org.kamranzafar.jtar.TarOutputStream;
import org.kitesdk.cli.TestUtil;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestTarImportCommand {

  private Logger console = null;
  private TarImportCommand command;

  private static String cwd;

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

  @BeforeClass
  public static void createTestInputFiles() throws IOException {
    TestTarImportCommand.cleanup();

    Path testData = new Path(TEST_DATA_DIR);
    FileSystem testFS = testData.getFileSystem(new Configuration());
    cwd = testFS.getWorkingDirectory().toString();

    TarOutputStream tosNoCompression = null;
    TarOutputStream tosGzipCompression = null;
    TarOutputStream tosBzip2Compression = null;
    XZCompressorOutputStream xzos = null;
    TarHeader rootDirHeader = null;
    try {
      // No compression
      tosNoCompression =
          new TarOutputStream(testFS.create(new Path(TAR_TEST_FILE), true));
      rootDirHeader = TarHeader
          .createHeader(TAR_TEST_ROOT_PREFIX, 0, System.currentTimeMillis(),
              true);
      tosNoCompression.putNextEntry(new TarEntry(rootDirHeader));

      // Gzip compression
      tosGzipCompression = new TarOutputStream(new GzipCompressorOutputStream(
          testFS.create(new Path(TAR_TEST_GZIP_FILE), true)));
      rootDirHeader = TarHeader.createHeader(TAR_TEST_GZIP_ROOT_PREFIX, 0,
          System.currentTimeMillis(), true);
      tosGzipCompression.putNextEntry(new TarEntry(rootDirHeader));

      // BZip2 compression
      tosBzip2Compression = new TarOutputStream(new BZip2CompressorOutputStream(
          testFS.create(new Path(TAR_TEST_BZIP2_FILE), true)));
      rootDirHeader = TarHeader.createHeader(TAR_TEST_BZIP2_ROOT_PREFIX, 0,
          System.currentTimeMillis(), true);
      tosBzip2Compression.putNextEntry(new TarEntry(rootDirHeader));

      // Generate test files with random names and content
      Random random = new Random(1);
      for (int i = 0; i < NUM_TEST_FILES; ++i) {
        // Create random file and data
        int fNameLength = random.nextInt(MAX_FILENAME_LENGTH);
        int fContentLength = random.nextInt(MAX_FILECONTENT_LENGTH);
        String fName = RandomStringUtils.randomAlphanumeric(fNameLength);
        String fContent = RandomStringUtils.randomAscii(fContentLength);

        // Write the file to tarball
        TestTarImportCommand.writeToTarFile(tosNoCompression,
            TAR_TEST_ROOT_PREFIX + "/" + fName, fContent);
        TestTarImportCommand.writeToTarFile(tosGzipCompression,
            TAR_TEST_GZIP_ROOT_PREFIX + "/" + fName, fContent);
        TestTarImportCommand.writeToTarFile(tosBzip2Compression,
            TAR_TEST_BZIP2_ROOT_PREFIX + "/" + fName, fContent);

        System.out.println("Wrote " + fName + " [" + fContentLength + "]");
      }
    } finally {
      IOUtils.closeStream(tosNoCompression);
      IOUtils.closeStream(tosGzipCompression);
      IOUtils.closeStream(tosBzip2Compression);
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

  private static void writeToTarFile(TarOutputStream tos, String name,
                                     String content) throws IOException {
    TarHeader tarHeader = TarHeader
        .createHeader(name, content.length(), System.currentTimeMillis(),
            false);
    TarEntry tarEntry = new TarEntry(tarHeader);
    tos.putNextEntry(tarEntry);
    byte[] buf = content.getBytes();
    tos.write(buf, 0, content.length());
    tos.flush();
  }

  @Test
  public void testTarNoCompressionImportCommand() throws IOException {
    command.targets = Lists.newArrayList(TAR_TEST_FILE, TEST_DATASET_NAME);
    command.compressionType = "none";
    command.run();
    verify(console).info("Added {} records to \"{}\"",
        NUM_TEST_FILES,
        "repo:" + cwd + "/" + TEST_DATASET_DIR + "::" + TEST_DATASET_NAME);
    //verifyNoMoreInteractions(console);
  }

  @Test
  public void testTarGzipImportCommand() throws IOException {
    command.targets = Lists.newArrayList(TAR_TEST_GZIP_FILE, TEST_DATASET_NAME);
    command.compressionType = "gzip";
    command.run();
    verify(console).info("Added {} records to \"{}\"",
        NUM_TEST_FILES,
        "repo:" + cwd + "/" + TEST_DATASET_DIR + "::" + TEST_DATASET_NAME);
    //verifyNoMoreInteractions(console);
  }

  @Test
  public void testTarBzip2ImportCommand() throws IOException {
    command.targets =
        Lists.newArrayList(TAR_TEST_BZIP2_FILE, TEST_DATASET_NAME);
    command.compressionType = "bzip2";
    command.run();
    verify(console).info("Added {} records to \"{}\"",
        NUM_TEST_FILES,
        "repo:" + cwd + "/" + TEST_DATASET_DIR + "::" + TEST_DATASET_NAME);
    //verifyNoMoreInteractions(console);
  }

  @Before
  public void setup() throws Exception {
    TestUtil.run("-v", "create", TEST_DATASET_NAME,
        "--use-local", "-d", TEST_DATASET_DIR,
        "-s",
        "kite-tools-parent/kite-tools/src/test/resources/test-schemas/" +
            "tar-import.avsc");
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
