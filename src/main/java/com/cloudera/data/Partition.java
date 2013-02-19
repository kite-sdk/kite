package com.cloudera.data;

import java.io.IOException;

public interface Partition<E> {

  String getName();

  PartitionExpression getExpression();

  DatasetReader<E> getReader() throws IOException;

  DatasetWriter<E> getWriter() throws IOException;

}
