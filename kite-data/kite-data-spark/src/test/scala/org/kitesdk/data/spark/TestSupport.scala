/*
 * Copyright 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spark

import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.kitesdk.data._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait TestSupport extends BeforeAndAfterEach with BeforeAndAfterAll {

  self: Suite =>

  @transient var sc: SparkContext = _

  protected def cleanup(): Unit = {
    val conf = new Configuration()
    val dir = new Path(s"${System.getProperty("user.dir")}/target/tmp/")
    val fileSystem = dir.getFileSystem(conf)
    if (fileSystem.exists(dir))
      fileSystem.delete(dir, true)
    ()
  }

  protected def generateDataset(format: Format, compressionType: CompressionType) = {
    val descriptor = new DatasetDescriptor.Builder().schemaUri("resource:product.avsc").compressionType(compressionType).format(format).build() //Snappy compression is the default
    val products = Datasets.create[GenericRecord, Dataset[GenericRecord]](s"dataset:file://${System.getProperty("user.dir")}/target/tmp/test/products", descriptor, classOf[GenericRecord])
    val writer = products.newWriter()
    val builder = new GenericRecordBuilder(descriptor.getSchema)
    for (i <- 1 to 100) {
      val record = builder.set("name", s"product-$i").set("id", i.toLong).build()
      writer.write(record)
    }
    writer.close()
    products
  }

  override def beforeAll() {
    super.beforeAll()
  }

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext(): Unit = {
    LocalSparkContext.stop(sc)
    sc = null
  }

}

object LocalSparkContext {
  def stop(sc: SparkContext) {
    if (sc != null) {
      sc.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }
}
