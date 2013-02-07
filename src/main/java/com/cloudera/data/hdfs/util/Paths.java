package com.cloudera.data.hdfs.util;

import java.io.File;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

public class Paths {

  public static File toFile(Path path) {
    Preconditions.checkArgument(path != null, "Path can not be null");

    return new File(path.toUri().getPath());
  }

}
