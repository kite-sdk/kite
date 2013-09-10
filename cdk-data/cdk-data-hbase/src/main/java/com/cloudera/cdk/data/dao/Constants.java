// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.dao;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase Common Library Constants 
  */
public class Constants {

  public static byte[] SYS_COL_FAMILY = Bytes.toBytes("_s");
  
  public static byte[] OBSERVABLE_COL_FAMILY = Bytes.toBytes("observable");
  
  public static byte[] VERSION_CHECK_COL_QUALIFIER = Bytes.toBytes("w");
}
