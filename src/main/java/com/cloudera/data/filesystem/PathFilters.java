package com.cloudera.data.filesystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

abstract class PathFilters {

  public static PathFilter notHidden() {
    return new PathFilter() {

      @Override
      public boolean accept(Path path) {
        return !(path.getName().startsWith(".") || path.getName().startsWith(
            "_"));
      }
    };
  }

}
