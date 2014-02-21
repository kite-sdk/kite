package org.kitesdk.data.mapreduce;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.filesystem.impl.Accessor;

public class DatasetKeyInputFormat<E> extends InputFormat<AvroKey<E>, NullWritable> {

  public static void setRepositoryUri(Job job, URI uri) {
    job.getConfiguration().set(DatasetKeyOutputFormat.KITE_REPOSITORY_URI,
        uri.toString());
  }

  public static void setDatasetName(Job job, String name) {
    job.getConfiguration().set(DatasetKeyOutputFormat.KITE_DATASET_NAME, name);
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
      InterruptedException {
    Job job = new Job(jobContext.getConfiguration());
    Dataset<E> dataset = loadDataset(jobContext);
    List<Path> paths = Lists.newArrayList(Accessor.getDefault().getPathIterator(dataset));
    FileInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));
    AvroJob.setInputKeySchema(job, dataset.getDescriptor().getSchema());
    AvroKeyInputFormat<E> delegate = new AvroKeyInputFormat<E>();
    return delegate.getSplits(jobContext);
  }

  @Override
  public RecordReader<AvroKey<E>, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    AvroKeyInputFormat<E> delegate = new AvroKeyInputFormat<E>();
    return delegate.createRecordReader(inputSplit, taskAttemptContext);
  }

  private static DatasetRepository getDatasetRepository(JobContext jobContext) {
    Configuration conf = jobContext.getConfiguration();
    return DatasetRepositories.open(conf.get(DatasetKeyOutputFormat.KITE_REPOSITORY_URI));
  }

  private static <E> Dataset<E> loadDataset(JobContext jobContext) {
    Configuration conf = jobContext.getConfiguration();
    DatasetRepository repo = getDatasetRepository(jobContext);
    return repo.load(conf.get(DatasetKeyOutputFormat.KITE_DATASET_NAME));
  }
}
