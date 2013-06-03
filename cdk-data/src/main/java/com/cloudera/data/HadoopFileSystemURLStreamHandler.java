package com.cloudera.data;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A {@link URLStreamHandler} for handling Hadoop filesystem URLs,
 * most commonly those with the <i>hdfs</i> scheme.
 */
class HadoopFileSystemURLStreamHandler extends URLStreamHandler {

  static class HadoopFileSystemURLConnection extends URLConnection {
    public HadoopFileSystemURLConnection(URL url) {
      super(url);
    }
    @Override
    public void connect() throws IOException {
    }
    @Override
    public InputStream getInputStream() throws IOException {
      Path path = new Path(url.toExternalForm());
      FileSystem fileSystem = path.getFileSystem(new Configuration());
      return fileSystem.open(path);
    }
  }

  @Override
  protected URLConnection openConnection(URL url) throws IOException {
    return new HadoopFileSystemURLConnection(url);
  }
}
