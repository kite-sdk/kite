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
package org.kitesdk.morphline.hadoop.core;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@RunWith(Parameterized.class)
public class DownloadHdfsFileTest extends MiniDFSTest {
  
  private String fileName;
  private FileSystem fileSystem;
  private boolean isDir;
  private Path testDirectory;
  private File dst;

  private static final String RESOURCES_DIR = "target/test-classes";
  
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { "foo.txt", getDFS(), false },
        { "food.txt", getDFS(), true },
        { "bar.txt", getFS(), false },
        { "bard.txt", getFS(), true },
    });
  }

  public DownloadHdfsFileTest(String fileName, FileSystem fs, boolean isDir) {
    this.fileName = fileName;
    this.fileSystem = fs;
    this.isDir = isDir;
  }

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
  }

  @After
  public void tearDown() throws IOException {
    if (dst != null) {
      if (isDir) {
        FileUtil.fullyDelete(dst.getParentFile());
      } else {
        FileUtil.fullyDelete(dst);
      }
    }
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testBasic() throws IOException {
    String msg = "hello world";

    // setup: copy a file to HDFS to prepare inputFile    
    Path inputFile = fileSystem.makeQualified(new Path(testDirectory, fileName));
    FSDataOutputStream out = fileSystem.create(inputFile);
    IOUtils.copyBytes(new ByteArrayInputStream(msg.getBytes(Charsets.UTF_8)), out, fileSystem.getConf());
    out.close();
    
    File cwd = Files.createTempDir().getAbsoluteFile();
    if (isDir) {
      dst = new File(cwd, testDirectory.getName() + "/" + inputFile.getName());
      inputFile = inputFile.getParent();
    } else {
      dst = new File(cwd, inputFile.getName());
    }
    Assert.assertFalse(dst.exists());
    new File(cwd, fileName).mkdirs(); // will be auto deleted!
    Files.write("wrong msg", new File(new File(cwd, fileName), fileName), Charsets.UTF_8); // will be auto deleted!

    Command morphline = createMorphline("test-morphlines/testDownloadHdfsFile", inputFile, cwd);         
    Assert.assertTrue(morphline.process(new Record()));    
    Assert.assertEquals(msg, Files.toString(dst, Charsets.UTF_8));
    if (isDir) {
      FileUtil.fullyDelete(dst.getParentFile());
    } else {
      FileUtil.fullyDelete(dst);
    }
    Assert.assertTrue(fileSystem.exists(inputFile));
    Assert.assertTrue(FileUtil.fullyDelete(cwd));
    
    // verify that subsequent calls with same inputFile won't copy the file again (to prevent races)
    morphline = createMorphline("test-morphlines/downloadHdfsFile", inputFile, cwd);       
    Assert.assertTrue(morphline.process(new Record()));    
    Assert.assertFalse(dst.exists());
    Assert.assertTrue(morphline.process(new Record()));
    Assert.assertFalse(dst.exists());
    Assert.assertFalse(cwd.exists());
    
    Assert.assertTrue(fileSystem.delete(inputFile, true));
    
    try {
      morphline = createMorphline("test-morphlines/downloadHdfsFile", 
          new Path("nonExistingInputFile"), cwd);    
      Assert.fail("failed to detect non-existing input file");
    } catch (MorphlineCompilationException e) {
      Assert.assertTrue(e.getCause() instanceof FileNotFoundException);
    }
    Assert.assertFalse(dst.exists());
  }

  private Command createMorphline(String file, Path inputFile, File cwd) {
    return createMorphline("test-morphlines/downloadHdfsFile", 
        ConfigFactory.parseMap(
            ImmutableMap.of("inputFile", inputFile.toString(), "outputDir", cwd.toString())));    
  }
  
  private Command createMorphline(String file, Config... overrides) {
    return new Compiler().compile(
        new File(RESOURCES_DIR + "/" + file + ".conf"), 
        null, 
        createMorphlineContext(), 
        null, 
        overrides);
  }

  private MorphlineContext createMorphlineContext() {
    return new MorphlineContext.Builder().setMetricRegistry(new MetricRegistry()).build();
  }

}
