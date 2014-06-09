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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

import org.kitesdk.morphline.shaded.com.google.common.io.Closeables;
import com.typesafe.config.Config;


/**
 * Opens an HDFS file for read and return a corresponding InputStream.
 */
public final class OpenHdfsFileBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("openHdfsFile");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new OpenHdfsFile(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class OpenHdfsFile extends AbstractCommand {

    private final Configuration conf;

    public OpenHdfsFile(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.conf = new Configuration();
      String defaultFileSystemUri = getConfigs().getString(config, "fs", null);
      if (defaultFileSystemUri != null) {
        FileSystem.setDefaultUri(conf, defaultFileSystemUri); // see Hadoop's GenericOptionsParser
      }
      for (String value : getConfigs().getStringList(config, "conf", Collections.<String>emptyList())) {
        conf.addResource(new Path(value)); // see Hadoop's GenericOptionsParser
      }
      validateArguments();
    }
  
    @Override
    protected boolean doProcess(Record record) {
      for (Object body : record.get(Fields.ATTACHMENT_BODY)) {
        Record outputRecord = record.copy();
        AbstractParser.removeAttachments(outputRecord);
        String pathString = body.toString();
        Path path = new Path(pathString);
        InputStream in = null;
        try {
          try {
            FileSystem fs = path.getFileSystem(conf);
            in = fs.open(path);
            if (pathString.endsWith(".gz")) {
              in = new GZIPInputStream(in, 64 * 1024);
            }
            in = new BufferedInputStream(in);
            outputRecord.put(Fields.ATTACHMENT_BODY, in);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
  
          // pass record to next command in chain:
          if (!getChild().process(outputRecord)) {
            return false;
          }
        } finally {
          Closeables.closeQuietly(in);
        }
      }
      return true;
    }
      
  }
}
