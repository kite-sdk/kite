package com.cloudera.data;

import java.io.IOException;

public interface DatasetWriter<E> {

  void open() throws IOException;

  void write(E entity) throws IOException;

  void flush() throws IOException;

  void close() throws IOException;

}
