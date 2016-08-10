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

import com.databricks.spark.avro.SchemaSupport._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Logging
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat
import org.kitesdk.data.{Dataset, Datasets}

class KiteDatasetRelation(private val parameters: Map[String, String],
                          private val df: Option[DataFrame] = None)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with Logging {

  val uri = parameters.get("uri")

  val dataSet: Option[Dataset[GenericRecord]] = if (df.isDefined) None else Some(Datasets.load(uri.get))

  val avroSchema: Option[Schema] = dataSet.map(_.getSchema)

  override def schema = getSchemaType(avroSchema.get)

  override def buildScan() = {
    val j = Job.getInstance()
    DatasetKeyInputFormat.configure(j).readFrom(dataSet.get).withType(classOf[GenericRecord])
    val rdd = sqlContext.sparkContext.newAPIHadoopRDD(j.getConfiguration,
      classOf[DatasetKeyInputFormat[GenericRecord]],
      classOf[GenericRecord], classOf[Void])
    val converter = createConverter(sqlContext, dataSet.get.getDescriptor.getSchema)
    rdd.map(record => converter(record._1).asInstanceOf[Row])
  }
}