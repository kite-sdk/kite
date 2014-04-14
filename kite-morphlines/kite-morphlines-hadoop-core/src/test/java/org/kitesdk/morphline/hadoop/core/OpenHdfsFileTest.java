/*
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
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.morphline.api.Collector;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Fields;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class OpenHdfsFileTest extends MiniDFSTest {
  
  private FileSystem fileSystem;
  private Path testDirectory;
  private Collector collector;

  private static final String RESOURCES_DIR = "target/test-classes";
  
  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    collector = new Collector();
  }

  @After
  public void tearDown() throws IOException {
    collector = null;
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testBasic() throws IOException {
    String msg = "hello world";

    // setup: copy a file to HDFS to prepare inputFile    
    Path inputFile = fileSystem.makeQualified(new Path(testDirectory, "foo.txt.gz"));
    OutputStream out = fileSystem.create(inputFile);
    out = new GZIPOutputStream(out);
    IOUtils.copyBytes(new ByteArrayInputStream(msg.getBytes(Charsets.UTF_8)), out, fileSystem.getConf());
    out.flush();
    out.close();
    Assert.assertTrue(fileSystem.exists(inputFile));

    Command morphline = createMorphline("test-morphlines/openHdfsFile");         
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, inputFile.toString());
    Assert.assertTrue(morphline.process(record));    
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    Assert.assertEquals(expected, collector.getFirstRecord());
  }

  private Command createMorphline(String file) {
    return new Compiler().compile(
        new File(RESOURCES_DIR + "/" + file + ".conf"), 
        null, 
        createMorphlineContext(), 
        collector); 
  }

  private MorphlineContext createMorphlineContext() {
    return new MorphlineContext.Builder().setMetricRegistry(new MetricRegistry()).build();
  }

}
