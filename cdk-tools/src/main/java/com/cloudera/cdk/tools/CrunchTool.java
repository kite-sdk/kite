/**
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
package com.cloudera.cdk.tools;

import java.io.Serializable;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * A partial copy of Crunch's tool with the getPipeline() method that we need to
 * specify a WriteMode - this is not exposed in Crunch 0.5.0. This class can be removed
 * when the next release of Crunch is available.
 */
abstract class CrunchTool extends Configured implements Tool, Serializable {

  private transient Pipeline pipeline = new MRPipeline(getClass());

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null && pipeline != null) {
      pipeline.setConfiguration(conf);
    }
  }

  @Override
  public Configuration getConf() {
    return pipeline.getConfiguration();
  }

  public PCollection<String> readTextFile(String pathName) {
    return pipeline.readTextFile(pathName);
  }

  public PipelineResult run() {
    return pipeline.run();
  }

  protected Pipeline getPipeline() {
    return pipeline;
  }
}
