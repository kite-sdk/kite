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

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.kitesdk.data._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{MustMatchers, WordSpec}

import scala.beans.BeanProperty
import scala.util.Random

case class Person(@BeanProperty name: String, @BeanProperty age: Int)

case class User(name: String, creationDate: Long, favoriteColor: String)

@RunWith(classOf[JUnitRunner])
class SparkKiteSpec extends WordSpec with MustMatchers with TestSupport {

  "Spark" must {
    "be able to create a SchemaRDD/Dataframe from a kite parquet dataset" in {

      val conf = new SparkConf().
        setAppName("spark-kite-spec-test").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        setMaster("local")
      sc = new SparkContext(conf)

      cleanup()

      val products: Dataset[GenericRecord] = generateDataset(Formats.PARQUET, CompressionType.Snappy)

      val sqlContext = new SQLContext(sc)

      val data = sqlContext.kiteDatasetFile(products)

      data.registerTempTable("product")

      val res = sqlContext.sql("select * from product where id < 10")

      res.map(row => (row.getAs[String](0), row.getAs[Long](1))).collect() must be(
        for {
          i <- 1 to 9
        } yield (s"product-$i", i.toLong)
      )

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a SchemaRDD/Dataframe from a kite avro dataset" in {

      val conf = new SparkConf().
        setAppName("spark-kite-spec-test").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        setMaster("local")
      sc = new SparkContext(conf)

      cleanup()

      val products = generateDataset(Formats.AVRO, CompressionType.Snappy)

      val sqlContext = new SQLContext(sc)

      val data = sqlContext.kiteDatasetFile(products)

      data.registerTempTable("product")

      val res = sqlContext.sql("select * from product where id < 10")

      res.map(row => (row.getAs[String](0), row.getAs[Long](1))).collect() must be(
        for {
          i <- 1 to 9
        } yield (s"product-$i", i.toLong)
      )

      cleanup()

    }
  }

  private def testCreateKiteDataset(format: Format): Unit = {

    val conf = new SparkConf().
      setAppName("spark-kite-spec-test").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      setMaster("local")
    sc = new SparkContext(conf)

    cleanup()

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val datasetURI = URIBuilder.build(s"repo:file:////${System.getProperty("user.dir")}/target/tmp", "test", "persons")

    val peopleList = List(Person("David", 50), Person("Ruben", 14), Person("Giuditta", 12), Person("Vita", 19))
    val people = sc.parallelize[Person](peopleList).toDF()
    people.registerTempTable("people")

    val teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    val descriptor = new SparkDatasetDescriptor.Builder().dataFrame(teenagers).format(format).compressionType(CompressionType.Snappy).build()

    val dataset = KiteDatasetSaver.saveAsKiteDataset(descriptor, datasetURI)
    val reader = dataset.newReader()

    import collection.JavaConversions._
    reader.iterator().toList.sortBy(g => g.get("name").toString).mkString(",") must be("{\"name\": \"Ruben\", \"age\": 14},{\"name\": \"Vita\", \"age\": 19}")
    reader.close()

    cleanup()

  }

  "Spark" must {
    "be able to create a kite parquet dataset from a SchemaRDD/Dataframe" in {

      testCreateKiteDataset(Formats.PARQUET)

    }
  }

  "Spark" must {
    "be able to create a kite avro dataset from a SchemaRDD/Dataframe" in {

      testCreateKiteDataset(Formats.AVRO)

    }
  }

  "Spark" must {
    "be able to append a SchemaRDD/Dataframe to an existing dataset" in {

      val conf = new SparkConf().
        setAppName("spark-kite-spec-test").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        setMaster("local")
      sc = new SparkContext(conf)
      cleanup()

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val datasetURI = URIBuilder.build(s"repo:file:////${System.getProperty("user.dir")}/target/tmp", "test", "persons1")

      val peopleList1 = List(Person("David", 50), Person("Ruben", 14))
      val people1 = sc.parallelize[Person](peopleList1).toDF()

      val peopleList2 = List(Person("Giuditta", 12), Person("Vita", 19))
      val people2 = sc.parallelize[Person](peopleList2).toDF()

      val descriptor = new SparkDatasetDescriptor.Builder().dataFrame(people1).format(Formats.AVRO).compressionType(CompressionType.Snappy).build()
      val dataset1 = KiteDatasetSaver.saveAsKiteDataset(descriptor, datasetURI)

      val dataset2 = KiteDatasetSaver.saveAsKiteDataset(people2, dataset1, WriteMode.APPEND)

      val reader = dataset2.newReader()

      import collection.JavaConversions._

      reader.
        iterator().
        toList.map(row => Person(row.get(0).toString, row.get(1).asInstanceOf[Int])).sortBy(_.name) must be((peopleList1 ++ peopleList2).sortBy(_.name))

      reader.close()

      cleanup()

    }
  }

  "Spark" must {
    "be able to overwrite with SchemaRDD/Dataframe an existing dataset" in {

      val conf = new SparkConf().
        setAppName("spark-kite-spec-test").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        setMaster("local")
      sc = new SparkContext(conf)

      cleanup()

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val datasetURI = URIBuilder.build(s"repo:file:////${System.getProperty("user.dir")}/target/tmp", "test", "persons2")

      val peopleList1 = List(Person("David", 50), Person("Ruben", 14))
      val people1 = sc.parallelize[Person](peopleList1).toDF()

      val peopleList2 = List(Person("Giuditta", 12), Person("Vita", 19))
      val people2 = sc.parallelize[Person](peopleList2).toDF()

      val descriptor = new SparkDatasetDescriptor.Builder().dataFrame(people1).format(Formats.AVRO).compressionType(CompressionType.Snappy).build()
      val dataset1 = KiteDatasetSaver.saveAsKiteDataset(descriptor, datasetURI)

      val dataset2 = KiteDatasetSaver.saveAsKiteDataset(people2, dataset1, WriteMode.OVERWRITE)

      val reader = dataset2.newReader()

      import collection.JavaConversions._

      reader.
        iterator().
        toList.map(row => Person(row.get(0).toString, row.get(1).asInstanceOf[Int])).sortBy(_.name) must be(peopleList2.sortBy(_.name))

      reader.close()

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a SchemaRDD/Dataframe from a kite avro dataset using a reflection based schema" in {

      val conf = new SparkConf().
        setAppName("spark-kite-spec-test").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        setMaster("local")
      sc = new SparkContext(conf)

      cleanup()

      val sqlContext = new SQLContext(sc)

      val datasetURI = URIBuilder.build(s"repo:file:////${System.getProperty("user.dir")}/target/tmp", "test", "persons3")

      val descriptor = new DatasetDescriptor.Builder().schema(classOf[Person]).format(Formats.AVRO).build()

      val peopleDataset = Datasets.create[Person, Dataset[Person]](datasetURI, descriptor, classOf[Person])

      val writer = peopleDataset.newWriter()

      val peopleList = (1 to 1000).map(i => Person(s"person-$i", Random.nextInt(80)))
      peopleList.foreach(writer.write)
      writer.close()

      val data = sqlContext.kiteDatasetFile(peopleDataset)

      data.collect().sortBy(_.getAs[String](0).split("-")(1).toInt).map(row => Person(row.getAs[String](0), row.getAs[Int](1))) must be(peopleList)

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a SchemaRDD/Dataframe from a partitioned kite avro dataset" in {

      val conf = new SparkConf().
        setAppName("spark-kite-spec-test").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        setMaster("local")
      sc = new SparkContext(conf)

      cleanup()

      val sqlContext = new SQLContext(sc)

      val partitionStrategy = new PartitionStrategy.Builder().identity("favoriteColor", "favorite_color").build()

      val datasetURI = URIBuilder.build(s"repo:file:////${System.getProperty("user.dir")}/target/tmp", "test", "users1")

      val descriptor = new DatasetDescriptor.Builder().schemaUri("resource:user.avsc").partitionStrategy(partitionStrategy).format(Formats.AVRO).build()

      val userDataset = Datasets.create[Record, Dataset[Record]](datasetURI, descriptor, classOf[Record])

      val colors = Array[String]("green", "blue", "pink", "brown", "yellow")
      val rand = new Random()
      val builder = new GenericRecordBuilder(descriptor.getSchema)
      val users = for {
        i <- 0 until 100
        fields = ("user-" + i, System.currentTimeMillis(), colors(rand.nextInt(colors.length)))
      } yield fields

      val writer = userDataset.newWriter()
      users.foreach(fields => {
        val record = builder.set("username", fields._1).set("creationDate", fields._2).set("favoriteColor", fields._3).build()
        writer.write(record)
      })
      writer.close()

      val data = sqlContext.kiteDatasetFile(userDataset)
      data.collect().sortBy(_.getAs[String](0).split("-")(1).toInt).
        map(record => (record.getAs[String](0), record.getAs[Long](1), record.getAs[String](2))) must be(users)

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a partitioned kite avro dataset from a SchemaRDD/Dataframe" in {

      val conf = new SparkConf().
        setAppName("spark-kite-spec-test").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        setMaster("local")
      sc = new SparkContext(conf)

      cleanup()

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val datasetURI = URIBuilder.build(s"repo:file:////${System.getProperty("user.dir")}/target/tmp", "test", "users2")

      val colors = Array[String]("green", "blue", "pink", "brown", "yellow")
      val rand = new Random()
      val usersList = (1 to 100).map(i => User("user-" + i, System.currentTimeMillis(), colors(rand.nextInt(colors.length))))
      val users = sc.parallelize[User](usersList).toDF()
      users.registerTempTable("user")

      val partitionStrategy = new PartitionStrategy.Builder().identity("favoriteColor", "favorite_color").build()
      val descriptor = new SparkDatasetDescriptor.Builder().dataFrame(users).format(Formats.AVRO).compressionType(CompressionType.Snappy).partitionStrategy(partitionStrategy).build()
      val dataset = KiteDatasetSaver.saveAsKiteDataset(descriptor, datasetURI)

      val reader = dataset.newReader()
      import collection.JavaConversions._
      reader.iterator().toList.sortBy(_.get(0).toString.split("-")(1).toInt).map(record => User(record.get(0).toString, record.get(1).asInstanceOf[Long], record.get(2).toString)) must be(usersList)
      reader.close()

      val ds2 = KiteDatasetSaver.saveAsKiteDataset(users, dataset, WriteMode.OVERWRITE)
      val reader2 = ds2.newReader()
      import collection.JavaConversions._
      reader2.iterator().toList.map(record => User(record.get(0).toString, record.get(1).asInstanceOf[Long], record.get(2).toString)).foreach(println)
      reader2.close()

      cleanup()

    }
  }

}
