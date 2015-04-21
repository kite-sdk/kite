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
package org.kitesdk.data.crunch;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.Map;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.MapReduceTarget;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.View;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitesdk.data.spi.LastModifiedAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DatasetTarget<E> implements MapReduceTarget {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetTarget.class);

  private View view;
  private final URI uri;
  private final FormatBundle formatBundle;

  public DatasetTarget(View<E> view) {
    this.view = view;
    Configuration temp = emptyConf();
    // use appendTo since handleExisting checks for existing data
    DatasetKeyOutputFormat.configure(temp).appendTo(view);
    this.formatBundle = outputBundle(temp);
    this.uri = view.getDataset().getUri();
  }

  public DatasetTarget(URI uri) {
    this.uri = uri;
    Configuration temp = emptyConf();
    // use appendTo since handleExisting checks for existing data
    DatasetKeyOutputFormat.configure(temp).appendTo(uri);
    this.formatBundle = outputBundle(temp);
  }

  @Override
  public Target outputConf(String key, String value) {
    formatBundle.set(key, value);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean handleExisting(WriteMode writeMode, long lastModForSource,
      Configuration entries) {
    outputConf(
        DatasetKeyOutputFormat.KITE_WRITE_MODE,
        kiteWriteMode(writeMode).toString());

    if (view == null) {
      try {
        view = Datasets.load(uri);
      } catch (DatasetNotFoundException e) {
        LOG.info("Writing to new dataset/view: " + uri);
        return true;
      }
    }

    boolean ready = false;
    if (view instanceof Signalable) {
      ready = ((Signalable)view).isReady();
    }
    // a view exists if it isn't empty, or if it has been marked ready
    boolean exists = ready || !view.isEmpty();
    if (exists) {
      switch (writeMode) {
        case DEFAULT:
          LOG.error("Dataset/view " + view + " already exists!");
          throw new CrunchRuntimeException("Dataset/view already exists: " + view);
        case OVERWRITE:
          LOG.info("Overwriting existing dataset/view: " + view);
          break;
        case APPEND:
          LOG.info("Appending to existing dataset/view: " + view);
          break;
        case CHECKPOINT:
          long lastModForTarget = -1;
          if (view instanceof LastModifiedAccessor) {
            lastModForTarget = ((LastModifiedAccessor) view).getLastModified();
          }

          if (ready && (lastModForTarget > lastModForSource)) {
            LOG.info("Re-starting pipeline from checkpoint dataset/view: " + view);
            break;
          } else {
            if (!ready) {
              LOG.info("Checkpoint is not ready. Deleting data from existing " +
                  "checkpoint dataset/view: " + view);
            } else {
              LOG.info("Source data has recent updates. Deleting data from existing " +
                  "checkpoint dataset/view: " + view);
            }
            delete(view);
            return false;
          }
        default:
          throw new CrunchRuntimeException("Unknown WriteMode:  " + writeMode);
      }
    } else {
      LOG.info("Writing to empty dataset/view: " + view);
    }
    return exists;
  }

  private DatasetKeyOutputFormat.WriteMode kiteWriteMode(WriteMode mode) {
    switch (mode) {
      case DEFAULT:
        return DatasetKeyOutputFormat.WriteMode.DEFAULT;
      case APPEND:
        return DatasetKeyOutputFormat.WriteMode.APPEND;
      case OVERWRITE:
        return DatasetKeyOutputFormat.WriteMode.OVERWRITE;
      default:
        // use APPEND and enforce in handleExisting
        return DatasetKeyOutputFormat.WriteMode.APPEND;
    }
  }

  private void delete(View view) {
    try {
      boolean deleted = view.deleteAll();
      if (!deleted) {
        LOG.warn("No data was deleted.");
      }
    } catch (UnsupportedOperationException e) {
      LOG.error("Dataset view " + view + " cannot be deleted!");
      throw new CrunchRuntimeException("Dataset view cannot be deleted:" + view, e);
    }
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (!(ptype instanceof AvroType)) {
      return false;
    }
    handler.configure(this, ptype);
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Converter<?, ?, ?, ?> getConverter(PType<?> ptype) {
    if (ptype instanceof AvroType) {
      return new KeyConverter<E>((AvroType<E>) ptype);
    }
    throw new DatasetException(
        "Cannot create converter for non-Avro type: " + ptype);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (ptype instanceof AvroType) {
      if (view != null) {
        return new DatasetSourceTarget(view, (AvroType) ptype);
      } else if (uri != null) {
        return new DatasetSourceTarget(uri, (AvroType) ptype);
      }
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {

    Preconditions.checkNotNull(name, "Output name should not be null"); // see CRUNCH-82

    Converter converter = getConverter(ptype);
    Class<?> keyClass = converter.getKeyClass();
    Class<?> valueClass = Void.class;

    CrunchOutputs.addNamedOutput(job, name, formatBundle, keyClass, valueClass);
    job.setOutputFormatClass(formatBundle.getFormatClass());
    formatBundle.configure(job.getConfiguration());
  }

  private static Configuration emptyConf() {
    return new Configuration(false /* do not load defaults */ );
  }

  /**
   * Builds a FormatBundle for DatasetKeyOutputFormat by copying a temp config.
   *
   * All properties will be copied from the temporary configuration
   *
   * @param conf A Configuration that will be copied
   * @return a FormatBundle with the contents of conf
   */
  private static FormatBundle<DatasetKeyOutputFormat> outputBundle(Configuration conf) {
    FormatBundle<DatasetKeyOutputFormat> bundle = FormatBundle
        .forOutput(DatasetKeyOutputFormat.class);
    for (Map.Entry<String, String> entry : conf) {
      bundle.set(entry.getKey(), entry.getValue());
    }
    return bundle;
  }

  /**
   * Returns a brief description of this {@code DatasetTarget}. The exact details of the
   * representation are unspecified and subject to change, but the following may be regarded
   * as typical:
   * <p>
   * "Kite(dataset:hdfs://host/path/to/repo)"
   *
   * @return  a string representation of the object.
   */
  @Override
  public String toString() {
    return "Kite(" + uri + ")";
  }

}
