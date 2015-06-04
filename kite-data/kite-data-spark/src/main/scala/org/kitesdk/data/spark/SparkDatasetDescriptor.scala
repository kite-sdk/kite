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

import java.io.{File, InputStream}
import java.net.{URI, URL}

import com.databricks.spark.avro.SchemaSupport
import org.apache.avro.Schema
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.kitesdk.data._

class SparkDatasetDescriptor(val dataFrame: DataFrame,
                             schemaUri: URI,
                             format: Format,
                             location: URI,
                             properties: java.util.Map[String, String],
                             partitionStrategy: PartitionStrategy,
                             columnMapping: ColumnMapping,
                             compressionType: CompressionType)
  extends DatasetDescriptor(
    getSchema(dataFrame.schema),
    schemaUri,
    format,
    location,
    properties,
    partitionStrategy,
    columnMapping,
    compressionType
  ) with SchemaSupport {


}

object SparkDatasetDescriptor {

  class Builder extends DatasetDescriptor.Builder {

    var dataFrame: Option[DataFrame] = None

    override def build(): SparkDatasetDescriptor = {
      ValidationException.check(
        dataFrame.isDefined,
        "Dataframe is required and cannot be null"
      )

      val baseDatasetDescriptor =
        new DatasetDescriptor.Builder().
          schema(getSchema(dataFrame.get.schema)).
          build()

      def toURI(url: URL): URI =
        if (url == null)
          null
        else
          URI.create(url.toExternalForm)

      import collection.JavaConversions._

      val properties =
        baseDatasetDescriptor.
          listProperties().
          map(prop => (prop, baseDatasetDescriptor.getProperty(prop))).
          toMap

      val datasetDescriptor = new SparkDatasetDescriptor(
        dataFrame.get,
        toURI(baseDatasetDescriptor.getSchemaUrl),
        baseDatasetDescriptor.getFormat,
        baseDatasetDescriptor.getLocation,
        properties,
        if (baseDatasetDescriptor.isPartitioned)
          baseDatasetDescriptor.getPartitionStrategy
        else
          null,
        if (baseDatasetDescriptor.isColumnMapped)
          baseDatasetDescriptor.getColumnMapping
        else
          null,
        baseDatasetDescriptor.getCompressionType
      )

      datasetDescriptor
    }

    def dataFrame(dataFrame: DataFrame): Builder = {
      this.dataFrame = Some(dataFrame)
      this
    }

    override def schema(schema: Schema) = {
      super.schema(schema)
      this
    }

    override def schema(file: File) = {
      super.schema(file)
      this
    }

    override def schema(in: InputStream) = {
      super.schema(in)
      this
    }

    override def schemaUri(uri: URI) = {
      super.schemaUri(uri)
      this
    }

    override def schemaUri(uri: String) = {
      super.schemaUri(uri)
      this
    }

    override def schemaLiteral(s: String) = {
      super.schemaLiteral(s)
      this
    }

    override def schema[T](`type`: Class[T]) = {
      super.schema(`type`)
      this
    }

    override def schemaFromAvroDataFile(file: File) = {
      super.schemaFromAvroDataFile(file)
      this
    }

    override def schemaFromAvroDataFile(in: InputStream) = {
      super.schemaFromAvroDataFile(in)
      this
    }

    override def schemaFromAvroDataFile(uri: URI) = {
      super.schemaFromAvroDataFile(uri)
      this
    }

    override def format(format: Format) = {
      super.format(format)
      this
    }

    override def format(formatName: String) = {
      super.format(formatName)
      this
    }

    override def location(uri: URI) = {
      super.location(uri)
      this
    }

    override def location(uri: Path) = {
      super.location(uri)
      this
    }

    override def location(uri: String) = {
      super.location(uri)
      this
    }

    override def property(name: String, value: String) = {
      super.property(name, value)
      this
    }

    override def partitionStrategy(partitionStrategy: PartitionStrategy) = {
      super.partitionStrategy(partitionStrategy)
      this
    }

    override def partitionStrategy(file: File) = {
      super.partitionStrategy(file)
      this
    }

    override def partitionStrategy(in: InputStream) = {
      super.partitionStrategy(in)
      this
    }

    override def partitionStrategyLiteral(literal: String) = {
      super.partitionStrategyLiteral(literal)
      this
    }

    override def partitionStrategyUri(uri: URI) = {
      super.partitionStrategyUri(uri)
      this
    }

    override def partitionStrategyUri(uri: String) = {
      super.partitionStrategyUri(uri)
      this
    }

    override def columnMapping(columnMappings: ColumnMapping) = {
      super.columnMapping(columnMappings)
      this
    }

    override def columnMapping(file: File) = {
      super.columnMapping(file)
      this
    }

    override def columnMapping(in: InputStream) = {
      super.columnMapping(in)
      this
    }

    override def columnMappingLiteral(literal: String) = {
      super.columnMappingLiteral(literal)
      this
    }

    override def columnMappingUri(uri: URI) = {
      super.columnMappingUri(uri)
      this
    }

    override def columnMappingUri(uri: String) = {
      super.columnMappingUri(uri)
      this
    }

    override def compressionType(compressionType: CompressionType) = {
      super.compressionType(compressionType)
      this
    }

    override def compressionType(compressionTypeName: String) = {
      super.compressionType(compressionTypeName)
      this
    }
  }

}
