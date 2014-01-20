/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.morphline.hadoop.core;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

/**
 * Command for transferring HDFS files, for example to help with centralized configuration file
 * management. On startup, the command downloads zero or more files or directory trees from HDFS to
 * the local file system.
 */
public final class DownloadHdfsFileBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("downloadHdfsFile");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    try {
      return new DownloadHdfsFile(this, config, parent, child, context);
    } catch (IOException e) {
      throw new MorphlineCompilationException("Cannot compile", config, e);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class DownloadHdfsFile extends AbstractCommand {
    
    // global lock; contains successfully copied file paths
    private static final Set<String> DONE = new HashSet(); 

    public DownloadHdfsFile(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) 
        throws IOException {
      
      super(builder, config, parent, child, context);
      List<String> uris = getConfigs().getStringList(config, "inputFiles", Collections.EMPTY_LIST); 
      File dstRootDir = new File(getConfigs().getString(config, "outputDir", "."));
      Configuration conf = new Configuration();
      String defaultFileSystemUri = getConfigs().getString(config, "fs", null);
      if (defaultFileSystemUri != null) {
        FileSystem.setDefaultUri(conf, defaultFileSystemUri); // see Hadoop's GenericOptionsParser
      }
      for (String value : getConfigs().getStringList(config, "conf", Collections.EMPTY_LIST)) {
        conf.addResource(new Path(value)); // see Hadoop's GenericOptionsParser
      }
      validateArguments();
      download(uris, conf, dstRootDir);
    }
    
    /*
     * To prevent races, we lock out other commands that delete and write the same local files,
     * and we only once delete and write any given file. This ensures that local file reads only
     * occur after local file writes are completed. E.g. this handles N parallel SolrSinks clones
     * in the same VM.
     * 
     * TODO: consider extending this scheme to add filesystem based locking (advisory) in order to
     * also lock out clones in other JVM processes on the same file system.
     */
    private void download(List<String> uris, Configuration conf, File dstRootDir) throws IOException {
      synchronized (DONE) { 
        for (String uri : uris) {
          Path path = new Path(uri);
          File dst = new File(dstRootDir, path.getName()).getCanonicalFile();
          if (!DONE.contains(dst.getPath())) {
            if (dst.isDirectory()) {
              LOG.debug("Deleting dir {}", dst);
              FileUtils.deleteDirectory(dst);
            }
            FileSystem fs = path.getFileSystem(conf);
            if (fs.isFile(path)) {
              dst.getParentFile().mkdirs();
            }
            LOG.debug("Downloading {} to {}", uri, dst);
            if (!FileUtil.copy(fs, path, dst, false, conf)) {
              throw new IOException("Cannot download URI " + uri + " to " + dst);
            }
            DONE.add(dst.getPath());
            LOG.debug("Succeeded downloading {} to {}", uri, dst);
          }
        }
      }
    }
  }  
}
