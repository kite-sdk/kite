/*
 * Copyright 2014 Cloudera, Inc.
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
package org.kitesdk.data.mapreduce;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Format;
import scala.Tuple2;

@RunWith(Parameterized.class)
public class TestSpark extends FSTestBase {

  public TestSpark(Format format) {
    super(format);
  }

  // These are static inner classes becasuse Format does not implement Serializable
  public static class ToJava extends
      PairFunction<Tuple2<Record, Void>, String, Integer> {

    @Override
    public Tuple2<String, Integer> call(Tuple2<Record, Void> t) throws Exception {
      return new Tuple2<String, Integer>(t._1().get("text").toString(), 1);
    }
  }

  public static class Sum extends Function2<Integer, Integer, Integer> {

    @Override
    public Integer call(Integer t1, Integer t2) throws Exception {
      return t1 + t2;
    }
  }

  public static class ToAvro extends
      PairFunction<Tuple2<String, Integer>, Record, Void> {

    @Override
    public Tuple2<Record, Void> call(Tuple2<String, Integer> t) throws Exception {
      Record record = new Record(TestMapReduce.STATS_SCHEMA);
      record.put("name", t._1());
      record.put("count", t._2());

      return new Tuple2<Record, Void>(record, null);
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSparkJob() throws Exception {
    Dataset<Record> inputDataset = repo.create("in",
        new DatasetDescriptor.Builder()
          .property("kite.allow.csv", "true")
          .schema(TestMapReduce.STRING_SCHEMA)
          .format(format)
          .build(), Record.class);
    DatasetWriter<Record> writer = inputDataset.newWriter();
    writer.write(newStringRecord("apple"));
    writer.write(newStringRecord("banana"));
    writer.write(newStringRecord("banana"));
    writer.write(newStringRecord("carrot"));
    writer.write(newStringRecord("apple"));
    writer.write(newStringRecord("apple"));
    writer.close();


    Dataset<Record> outputDataset = repo.create("out",
        new DatasetDescriptor.Builder()
          .property("kite.allow.csv", "true")
          .schema(TestMapReduce.STATS_SCHEMA)
          .format(format)
          .build(), Record.class);

    Configuration conf = new Configuration();
    DatasetKeyInputFormat.configure(conf).readFrom(inputDataset);
    DatasetKeyOutputFormat.configure(conf).writeTo(outputDataset);



    @SuppressWarnings("unchecked")
    JavaPairRDD<Record, Void> inputData = SparkTestHelper.getSc().newAPIHadoopRDD(conf,
        DatasetKeyInputFormat.class, Record.class, Void.class);

    JavaPairRDD<String, Integer> mappedData = inputData.map(new ToJava());
    JavaPairRDD<String, Integer> sums = mappedData.reduceByKey(new Sum());
    JavaPairRDD<Record, Void> outputData = sums.map(new ToAvro());

    outputData.saveAsNewAPIHadoopFile("dummy", Record.class, Void.class,
        DatasetKeyOutputFormat.class, conf);

    DatasetReader<Record> reader = outputDataset.newReader();
    Map<String, Integer> counts = new HashMap<String, Integer>();
    for (Record record : reader) {
      counts.put(record.get("name").toString(), (Integer) record.get("count"));
    }
    reader.close();

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());

  }
}
