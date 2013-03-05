package com.cloudera.data.filesystem.util;

import java.io.File;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

public class Paths {

  /* Disallow instantiation. */
  private Paths() {
  }

  public static File toFile(Path path) {
    Preconditions.checkArgument(path != null, "Path can not be null");

    return new File(path.toUri().getPath());
  }

}
