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
package com.cloudera.cdk.examples.data;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.DatasetRepository;
import com.cloudera.data.DatasetWriter;
import com.cloudera.data.filesystem.FileSystemDatasetRepository;
import java.io.IOException;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Create a dataset on the local filesystem and write some {@link User} objects to it.
 */
public class CreateUserDatasetPojo extends Configured implements Tool {

  @Override
  public int run(String[] args) throws IOException {

    // Construct a local filesystem dataset repository rooted at /tmp/data
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path root = new Path("/tmp/data");
    DatasetRepository repo = new FileSystemDatasetRepository(fs, root);

    // Generate an Avro schema from the User class
    Schema schema = ReflectData.get().getSchema(User.class);

    // Create a dataset of users with the Avro schema in the repository
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder().schema(schema).get();
    Dataset users = repo.create("users", descriptor);

    // Get a writer for the dataset and write some users to it
    DatasetWriter<User> writer = users.getWriter();
    try {
      writer.open();
      String[] colors = { "green", "blue", "pink", "brown", "yellow" };
      Random rand = new Random();
      for (int i = 0; i < 100; i++) {
        User user = new User();
        user.setUsername("user-" + i);
        user.setCreationDate(System.currentTimeMillis());
        user.setFavoriteColor(colors[rand.nextInt(colors.length)]);
        writer.write(user);
      }
    } finally {
      writer.close();
    }

    return 0;
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new CreateUserDatasetPojo(), args);
    System.exit(rc);
  }
}
