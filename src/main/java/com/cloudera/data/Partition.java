package com.cloudera.data;

interface Partition {

  String getName();

  PartitionExpression getExpression();

  void getReader();

  void getWriter();

}
