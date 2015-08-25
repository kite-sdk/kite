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

import java.io.File
import java.net.URI

import com.databricks.spark.avro.SchemaSupport._
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.sources.{CreatableRelationProvider, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.kitesdk.data._
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat
import org.kitesdk.data.spi.DefaultConfiguration

class DefaultSource extends RelationProvider with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]) =
    new KiteDatasetRelation(parameters)(sqlContext)

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame) = {

    val builder = new DatasetDescriptor.Builder()

    val uri = parameters.get("uri").map(uri => new URI(uri))

    assert(uri.isDefined, "uri parameter has not been defined")

    assert(URIBuilder.DATASET_SCHEME == uri.get.getScheme, s"Not a dataset or view URI: $uri")

    val job = Job.getInstance(DefaultConfiguration.get())

    parameters.get("format").foreach(format => {
      builder.format(format)
    })

    parameters.get("compressionType").foreach(compressionType => {
      builder.compressionType(compressionType)
    })

    val p1 = parameters.get("partitionStrategyFile").
      map(partitionStrategyFile => builder.partitionStrategy(new File(partitionStrategyFile)))

    val p2 = parameters.get("partitionStrategyUri").
      map(partitionStrategyUri => builder.partitionStrategyUri(partitionStrategyUri))

    val p3 = parameters.get("partitionStrategyLiteral").
      map(partitionStrategyLiteral => builder.partitionStrategyLiteral(partitionStrategyLiteral))

    /*
  TODO this is an hack, the partitioning strategy only works with primitive
  type not nullable, if the schema has been built using reflection from a
  case class for example, the primitive types are nullable in the avro schema
  breaking the partition strategy, so in case the partition strategy is defined
  I change nullable to false, it seems working.
  */
    val schema: StructType = if (p1.isDefined || p2.isDefined || p3.isDefined)
      StructType(data.schema.iterator.map(field =>
        if (field.nullable) field.copy(nullable = false) else field).toArray)
    else
      data.schema

    val avroSchema = getSchema(schema)

    val descriptor: DatasetDescriptor = builder.schema(avroSchema).build()

    mode match {
      case SaveMode.ErrorIfExists =>
        if (Datasets.exists(uri.get)) {
          val dataset = Datasets.load[Dataset[GenericRecord]](uri.get)
          throw new RuntimeException(s"Dataset/view already exists: ${dataset.toString}")
        }
        else {
          val dataset = Datasets.create[GenericRecord, Dataset[GenericRecord]](
            uri.get,
            descriptor,
            classOf[GenericRecord]
          )
          DatasetKeyOutputFormat.configure(job).writeTo(dataset)
        }
        data.mapPartitions(rowsToAvro(_, schema)).saveAsNewAPIHadoopDataset(job.getConfiguration)
        createRelation(sqlContext, parameters)
      case SaveMode.Append =>
        val dataset = Datasets.load[Dataset[GenericRecord]](uri.get)
        DatasetKeyOutputFormat.configure(job).appendTo(dataset)
        data.mapPartitions(rowsToAvro(_, schema)).saveAsNewAPIHadoopDataset(job.getConfiguration)
        createRelation(sqlContext, parameters)
      case SaveMode.Overwrite =>
        val dataset = Datasets.load[Dataset[GenericRecord]](uri.get)
        DatasetKeyOutputFormat.configure(job).overwrite(dataset)
        data.mapPartitions(rowsToAvro(_, schema)).saveAsNewAPIHadoopDataset(job.getConfiguration)
        createRelation(sqlContext, parameters)
      case SaveMode.Ignore =>
        if (Datasets.exists(uri.get))
          createRelation(sqlContext, parameters)
        else
          throw new RuntimeException(s"Dataset/view does not exist: ${uri.toString}")
    }
  }
}