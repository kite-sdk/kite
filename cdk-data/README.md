# Cloudera Development Kit - HDFS Data Module

The HDFS Data Module is a collection of APIs that drastically simplify
working with datasets in the Hadoop Distributed FileSystem (HDFS).

This project is part of the Cloudera Development Kit (CDK), an open source
set of APIs that helps developers build robust systems and applications
with CDH and Cloudera Manager.

Category: Storage and Access

CDK Module: data.hdfs

## Example

The follow example uses the HDFS Data module to create a dataset called
`users`, open a writer to it, and write 100 `User` entities. The `User` entity
is a Scala case class which just means a bunch of convenience methods are
generated for us. It's akin to a typical Java model-style POJO.

```
esammer:~/Documents/Code/data-hdfs esammer$ cat test.scala
import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import com.google.common.collect.Lists

import com.cloudera.data.Dataset
import com.cloudera.data.hdfs.HDFSDatasetRepository
import com.cloudera.data.hdfs.HDFSDatasetWriter

case class User(id: Long, username: String, tags: Array[String])

// Create an HDFS dataset repo rooted at file:///tmp/data
val repo = new HDFSDatasetRepository(
  FileSystem.get(new Configuration()),
  new Path("/tmp/data")
)

// Create an Avro schema that corresponds with the User entity
val userSchema = Schema.createRecord("user", null, null, false)

userSchema.setFields(
  Lists.newArrayList(
    new Field("id", Schema.create(Type.LONG), null, null),
    new Field("username", Schema.create(Type.STRING), null, null),
    new Field("tags", Schema.createArray(Schema.create(Type.STRING)), null, null)
  )
)

// Create a dataset named users with the userSchema
val users = repo.create("users", userSchema)

println("Writing to users dataset:" + users)

// Get a writer to the dataset. Currently requires a cast (we'll fix that).
val writer = users.getWriter().asInstanceOf[HDFSDatasetWriter[User]]

writer.open()

for (i <- 0 to 100) {
  // Write a bunch of entities!
  writer.write(User(i.toLong, "user-" + i, Array("one", "two", "three")))

  // Flush every once in a while.
  if (i % 10 == 0) {
    writer.flush()
  }
}

// Close the dataset
writer.close()
esammer:~/Documents/Code/data-hdfs esammer$ scala -cp ...blagh.. test.scala
2013-02-08 11:38:15,486 (main) [DEBUG - com.cloudera.data.hdfs.HDFSDatasetRepository.create(HDFSDatasetRepository.java:57)] Creating dataset:users schema:{"type":"record","name":"user","fields":[{"name":"id","type":"long"},{"name":"username","type":"string"},{"name":"tags","type":{"type":"array","items":"string"}}]} datasetPath:/tmp/data/users/data
2013-02-08 11:38:15,490 (main) [DEBUG - com.cloudera.data.hdfs.HDFSDatasetRepository.create(HDFSDatasetRepository.java:66)] Serializing dataset schema:{"type":"record","name":"user","fields":[{"name":"id","type":"long"},{"name":"username","type":"string"},{"name":"tags","type":{"type":"array","items":"string"}}]}
Writing to users dataset:HDFSDataset{name=users, schema={"type":"record","name":"user","fields":[{"name":"id","type":"long"},{"name":"username","type":"string"},{"name":"tags","type":{"type":"array","items":"string"}}]}, dataDirectory=/tmp/data/users/data, properties={}}
2013-02-08 11:38:15,509 (main) [DEBUG - com.cloudera.data.hdfs.HDFSDatasetWriter.open(HDFSDatasetWriter.java:41)] Opening data file with pathTmp:/tmp/data/users/data/1360351538973-1.tmp (final path will be path:/tmp/data/users/data/1360351538973-1)
2013-02-08 11:38:15,584 (main) [DEBUG - com.cloudera.data.hdfs.HDFSDatasetWriter.close(HDFSDatasetWriter.java:61)] Closing pathTmp:/tmp/data/users/data/1360351538973-1.tmp
2013-02-08 11:38:15,585 (main) [DEBUG - com.cloudera.data.hdfs.HDFSDatasetWriter.close(HDFSDatasetWriter.java:65)] Commiting pathTmp:/tmp/data/users/data/1360351538973-1.tmp to path:/tmp/data/users/data/1360351538973-1
esammer:~/Documents/Code/data-hdfs esammer$ find /tmp/data/
/tmp/data/
/tmp/data//users
/tmp/data//users/data
/tmp/data//users/data/1360351538973-1
/tmp/data//users/schema.avsc
esammer:~/Documents/Code/data-hdfs esammer$ cat /tmp/data/users/schema.avsc 
{"type":"record","name":"user","fields":[{"name":"id","type":"long"},{"name":"username","type":"string"},{"name":"tags","type":{"type":"array","items":"string"}}]}
esammer:~/Documents/Code/data-hdfs esammer$ ~/bin/avro-tool tojson /tmp/data/users/data/1360351538973-1 | head -n 3
{"id":0,"username":"user-0","tags":["one","two","three"]}
{"id":1,"username":"user-1","tags":["one","two","three"]}
{"id":2,"username":"user-2","tags":["one","two","three"]}
```

## License

Copyright 2013 Cloudera Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
