import com.cloudera.data.{DatasetDescriptor, DatasetWriter}
import com.cloudera.data.filesystem.FileSystemDatasetRepository
import java.io.FileInputStream
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.compat.Platform
import scala.util.Random

// Construct a local filesystem dataset repository rooted at /tmp/data
val repo = new FileSystemDatasetRepository(
  FileSystem.get(new Configuration()),
  new Path("/tmp/data")
)

// Read an Avro schema from the user.avsc file on the classpath
val schema = new Parser().parse(new FileInputStream("src/main/resources/user.avsc"))

// Create a dataset of users with the Avro schema in the repository
val descriptor = new DatasetDescriptor.Builder().schema(schema).get()
val users = repo.create("users", descriptor)

// Get a writer for the dataset and write some users to it
val writer = users.getWriter().asInstanceOf[DatasetWriter[GenericRecord]]
writer.open()
val colors = Array("green", "blue", "pink", "brown", "yellow")
val rand = new Random()
for (i <- 0 to 100) {
  val builder = new GenericRecordBuilder(schema)
  val record = builder.set("username", "user-" + i)
    .set("creationDate", Platform.currentTime)
    .set("favoriteColor", colors(rand.nextInt(colors.length))).build()
  writer.write(record)
}
writer.close()