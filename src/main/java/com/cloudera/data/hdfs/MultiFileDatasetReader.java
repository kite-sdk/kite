package com.cloudera.data.hdfs;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.data.DatasetReader;

public class MultiFileDatasetReader<E> implements DatasetReader<E> {

  private FileSystem fileSystem;
  private Schema schema;

  private Iterator<Path> filesIter;
  private HDFSDatasetReader<E> reader;

  public MultiFileDatasetReader(FileSystem fileSystem, List<Path> files,
      Schema schema) {

    this.fileSystem = fileSystem;
    this.schema = schema;
    this.filesIter = files.iterator();
  }

  @Override
  public void open() throws IOException {
    if (filesIter.hasNext()) {
      reader = new HDFSDatasetReader<E>(fileSystem, filesIter.next(), schema);
      reader.open();
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    while (true) {
      if (reader.hasNext()) {
        return true;
      } else {
        reader.close();
        reader = null;

        if (filesIter.hasNext()) {
          reader = new HDFSDatasetReader<E>(fileSystem, filesIter.next(),
              schema);
          reader.open();
        } else {
          return false;
        }
      }
    }
  }

  @Override
  public E read() throws IOException {
    return reader.read();
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

}