/**
 * Copyright 2014 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.mapreduce;

import com.google.common.annotations.Beta;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.InputFormatAccessor;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;

/**
 * A MapReduce {@code InputFormat} for reading from a {@link Dataset}.
 *
 * Since a {@code Dataset} only contains entities (not key/value pairs), this output
 * format ignores the value.
 *
 * @param <E> The type of entities in the {@code Dataset}.
 */
@Beta
public class DatasetKeyInputFormat<E> extends InputFormat<E, Void>
    implements Configurable {

  public static final String KITE_REPOSITORY_URI = "kite.inputRepositoryUri";
  public static final String KITE_DATASET_NAME = "kite.inputDatasetName";
  public static final String KITE_PARTITION_DIR = "kite.inputPartitionDir";
  public static final String KITE_CONSTRAINTS = "kite.inputConstraints";

  private Configuration conf;
  private InputFormat<E, Void> delegate;

  public static void setRepositoryUri(Job job, URI uri) {
    job.getConfiguration().set(KITE_REPOSITORY_URI, uri.toString());
  }

  public static void setDatasetName(Job job, String name) {
    job.getConfiguration().set(KITE_DATASET_NAME, name);
  }

  public static <E> void setView(Job job, View<E> view) {
    setView(job.getConfiguration(), view);
  }

  public static <E> void setView(Configuration conf, View<E> view) {
    if (view instanceof AbstractRefinableView) {
      conf.set(KITE_CONSTRAINTS, serialize(((AbstractRefinableView) view).getConstraints()));
    }
  }

  private static String serialize(Constraints constraints) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(baos);
      out.writeObject(constraints);
      out.close();
      return Base64.encodeBase64String(baos.toByteArray());
    } catch (IOException e) {
      throw new DatasetIOException("Cannot serialize constraints " + constraints, e);
    }
  }

  private static Constraints deserialize(String s) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(s));
      ObjectInputStream in = new ObjectInputStream(bais);
      return (Constraints) in.readObject();
    } catch (IOException e) {
      throw new DatasetIOException("Cannot deserialize constraints", e);
    } catch (ClassNotFoundException e) {
      throw new DatasetException("Cannot deserialize constraints", e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    Dataset<E> dataset = loadDataset(configuration);

    String partitionDir = conf.get(KITE_PARTITION_DIR);
    String constraintsString = conf.get(KITE_CONSTRAINTS);
    if (dataset.getDescriptor().isPartitioned() && partitionDir != null) {
      delegate = getDelegateInputFormatForPartition(dataset, partitionDir);
    } else if (constraintsString != null) {
      delegate = getDelegateInputFormatForView(dataset, constraintsString);
    } else {
      delegate = getDelegateInputFormat(dataset);
    }
  }

  @SuppressWarnings("unchecked")
  private InputFormat<E, Void> getDelegateInputFormat(View<E> view) {
    if (view instanceof InputFormatAccessor) {
      return ((InputFormatAccessor<E>) view).getInputFormat();
    }
    throw new UnsupportedOperationException("Implementation " +
          "does not provide InputFormat support. View: " + view);
  }

  private InputFormat<E, Void> getDelegateInputFormatForPartition(Dataset<E> dataset,
      String partitionDir) {
    PartitionKey key = ((FileSystemDataset<E>) dataset).keyFromDirectory(new Path(partitionDir));
    if (key != null) {
      return getDelegateInputFormat(dataset.getPartition(key, true));
    }
    throw new DatasetException("Cannot find partition " + partitionDir);
  }

  @SuppressWarnings("unchecked")
  private InputFormat<E, Void> getDelegateInputFormatForView(Dataset<E> dataset,
      String constraintsString) {
    Constraints constraints = deserialize(constraintsString);
    if (dataset instanceof AbstractDataset) {
      return getDelegateInputFormat(((AbstractDataset) dataset).filter(constraints));
    }
    throw new DatasetException("Cannot find view from constraints for " + dataset);
  }

  private static <E> Dataset<E> loadDataset(Configuration conf) {
    DatasetRepository repo = DatasetRepositories.open(conf.get(KITE_REPOSITORY_URI));
    return repo.load(conf.get(KITE_DATASET_NAME));
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
      justification="Delegate set by setConf")
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
      InterruptedException {
    return delegate.getSplits(jobContext);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
      justification="Delegate set by setConf")
  public RecordReader<E, Void> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return delegate.createRecordReader(inputSplit, taskAttemptContext);
  }

}
