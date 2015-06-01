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

package org.kitesdk.data.spark;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.hbase.HBaseDatasetRepositoryTest;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitesdk.data.mapreduce.HBaseTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestSparkHBase extends HBaseTestBase {

    private transient JavaSparkContext sc;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        sc = new JavaSparkContext("local", "JavaAPISuite");
    }

    @After
    public void tearDown() throws Exception {
        sc.stop();
        sc = null;
        super.after();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testSparkJob() throws Exception {

        String datasetName = tableName + ".TestGenericEntity";

        Dataset<GenericRecord> inputDataset = repo.create("default", "in",
                new DatasetDescriptor.Builder()
                        .schemaLiteral(testGenericEntity).build());

        DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
                .schemaLiteral(testGenericEntity)
                .build();
        Dataset<GenericRecord> outputDataset = repo.create("default", datasetName, descriptor);

        DatasetWriter<GenericRecord> writer = inputDataset.newWriter();
        try {
            for (int i = 0; i < 10; ++i) {
                GenericRecord entity = HBaseDatasetRepositoryTest.createGenericEntity(i);
                writer.write(entity);
            }
        } finally {
            writer.close();
        }

        Job job = Job.getInstance(HBaseTestUtils.getConf());
        DatasetKeyInputFormat.configure(job).readFrom(inputDataset);
        DatasetKeyOutputFormat.configure(job).writeTo(outputDataset);

        @SuppressWarnings("unchecked")
        JavaPairRDD<GenericRecord, Void> inputData = sc
                .newAPIHadoopRDD(job.getConfiguration(), DatasetKeyInputFormat.class,
                        GenericRecord.class, Void.class);

        inputData.saveAsNewAPIHadoopDataset(job.getConfiguration());

        int cnt = 0;
        DatasetReader<GenericRecord> reader = outputDataset.newReader();
        try {
            for (GenericRecord entity : reader) {
                HBaseDatasetRepositoryTest.compareEntitiesWithUtf8(cnt, entity);
                cnt++;
            }
            assertEquals(10, cnt);
        } finally {
            reader.close();
            assertFalse("Reader should be closed after calling close", reader.isOpen());
        }

    }

}
