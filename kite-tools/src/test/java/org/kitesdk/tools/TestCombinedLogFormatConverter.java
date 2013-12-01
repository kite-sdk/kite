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
package org.kitesdk.tools;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.filesystem.FileSystemDatasetRepository;
import com.google.common.io.Resources;
import java.io.File;
import junit.framework.Assert;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class TestCombinedLogFormatConverter {

  private static final File TEST_DIR = new File("/tmp", TestCombinedLogFormatConverter
      .class.getName());

  @Before
  public void setUp() {
    FileUtil.fullyDelete(TEST_DIR);
  }

  @Test
  public void test() throws Exception {
    CombinedLogFormatConverter tool = new CombinedLogFormatConverter();

    String input = Resources.getResource("access_log.txt").toExternalForm();
    String datasetRoot = TEST_DIR.toURI().toURL().toExternalForm();
    String datasetName = "logs";

    int exitCode = tool.run(input, datasetRoot, datasetName);

    Assert.assertEquals(0, exitCode);

    Path root = new Path(datasetRoot);
    FileSystem fs = root.getFileSystem(new Configuration());
    DatasetRepository repo = new FileSystemDatasetRepository.Builder().fileSystem(fs)
        .rootDirectory(root).build();
    Dataset<GenericRecord> dataset = repo.load(datasetName);
    DatasetReader<GenericRecord> reader = dataset.newReader();
    try {
      reader.open();
      Assert.assertTrue(reader.hasNext());
      GenericRecord first = reader.next();

      Assert.assertEquals("ip1", first.get("host"));
      Assert.assertNull(first.get("rfc931_identity"));
      Assert.assertNull(first.get("username"));
      Assert.assertEquals("24/Apr/2011:04:06:01 -0400", first.get("datetime"));
      Assert.assertEquals("GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1",
          first.get("request"));
      Assert.assertEquals(200, first.get("http_status_code"));
      Assert.assertEquals(40028, first.get("response_size"));
      Assert.assertNull(first.get("referrer"));
      Assert.assertEquals("Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex" +
          ".com/bots)", first.get("user_agent"));
    } finally {
      reader.close();
    }

  }
}
