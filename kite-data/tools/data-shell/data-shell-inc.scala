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

import java.io.File

import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import com.google.common.collect.Lists
import com.google.common.io.Files
import com.google.common.io.Resources

import com.cloudera.cdk.data._
import com.cloudera.cdk.data.partition._
import com.cloudera.cdk.data.filesystem._

val fileSystem = FileSystem.get(new Configuration())

object DataTools {

  def loadSchemaResource(name: String): Schema = {
    new Schema.Parser().parse(Resources.getResource(name).openStream())
  }

}
