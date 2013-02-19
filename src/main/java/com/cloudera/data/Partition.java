package com.cloudera.data;

import java.io.IOException;

public interface Partition<E> {

  DatasetReader<E> getReader() throws IOException;

  DatasetWriter<E> getWriter() throws IOException;

}
