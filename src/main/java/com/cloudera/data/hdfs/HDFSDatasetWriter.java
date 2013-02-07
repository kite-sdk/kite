package com.cloudera.data.hdfs;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.io.Closeables;

public class HDFSDatasetWriter<E> implements Flushable, Closeable {

  private static final Logger logger = LoggerFactory
      .getLogger(HDFSDatasetWriter.class);

  private Path path;
  private Schema schema;
  private FileSystem fileSystem;

  private Path pathTmp;
  private DataFileWriter<E> dataFileWriter;
  private DatumWriter<E> writer;

  public HDFSDatasetWriter(FileSystem fileSystem, Path path, Schema schema) {
    this.fileSystem = fileSystem;
    this.path = path;
    this.pathTmp = new Path(path.getParent(), path.getName() + ".tmp");
    this.schema = schema;
  }

  public void open() throws IOException {
    logger.debug(
        "Opening data file with pathTmp:{} (final path will be path:{})",
        pathTmp, path);

    writer = new ReflectDatumWriter<E>();
    dataFileWriter = new DataFileWriter<E>(writer);
    dataFileWriter.create(schema, new File(pathTmp.toUri().getPath()));
  }

  public void write(E entity) throws IOException {
    dataFileWriter.append(entity);
  }

  @Override
  public void flush() throws IOException {
    dataFileWriter.flush();
  }

  @Override
  public void close() throws IOException {
    logger.debug("Closing pathTmp:{}", pathTmp);

    Closeables.close(dataFileWriter, false);

    logger.debug("Commiting pathTmp:{} to path:{}", pathTmp, path);

    if (!fileSystem.rename(pathTmp, path)) {
      throw new IOException("Failed to move " + pathTmp + " to " + path);
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("path", path)
        .add("pathTmp", pathTmp).add("schema", schema).toString();
  }

}
