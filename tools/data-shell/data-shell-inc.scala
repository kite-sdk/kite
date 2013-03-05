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

import com.cloudera.data._
import com.cloudera.data.partition._
import com.cloudera.data.filesystem._
import com.cloudera.data.filesystem.util._

val fileSystem = FileSystem.get(new Configuration())

object DataTools {

  def loadSchemaResource(name: String): Schema = {
    new Schema.Parser().parse(Resources.getResource(name).openStream())
  }

}
