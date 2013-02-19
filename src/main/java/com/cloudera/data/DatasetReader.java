package com.cloudera.data;

import java.io.IOException;

public interface DatasetReader<E> {

  void open() throws IOException;

  boolean hasNext();

  E read() throws IOException;

  void close() throws IOException;

}
