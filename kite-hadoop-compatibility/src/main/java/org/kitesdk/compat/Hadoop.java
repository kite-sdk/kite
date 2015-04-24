/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.compat;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class Hadoop {

  public static boolean isHadoop1() {
    return !org.apache.hadoop.mapreduce.JobContext.class.isInterface();
  }

  public static class Job {
    public static final DynMethods.StaticMethod newInstance =
        new DynMethods.Builder("getInstance")
            .impl(org.apache.hadoop.mapreduce.Job.class, org.apache.hadoop.conf.Configuration.class)
            .ctorImpl(org.apache.hadoop.mapreduce.Job.class, org.apache.hadoop.conf.Configuration.class)
            .buildStatic();
  }

  public static class JobContext {
    public static final DynConstructors.
        Ctor<org.apache.hadoop.mapreduce.JobContext> ctor =
        new DynConstructors.Builder(org.apache.hadoop.mapreduce.TaskAttemptContext.class)
            .hiddenImpl(
                "org.apache.hadoop.mapreduce.task.JobContextImpl",
                org.apache.hadoop.conf.Configuration.class, JobID.class)
            .hiddenImpl(
                "org.apache.hadoop.mapreduce.JobContext",
                org.apache.hadoop.conf.Configuration.class, JobID.class)
            .build();

    public static final DynMethods.UnboundMethod getConfiguration =
        new DynMethods.Builder("getConfiguration")
            .impl(org.apache.hadoop.mapreduce.JobContext.class)
            .build();

    public static final DynMethods.UnboundMethod getJobID =
        new DynMethods.Builder("getJobID")
            .impl(org.apache.hadoop.mapreduce.JobContext.class)
            .build();
  }

  public static class TaskAttemptContext {
    public static final DynConstructors.
        Ctor<org.apache.hadoop.mapreduce.TaskAttemptContext> ctor =
        new DynConstructors.Builder(org.apache.hadoop.mapreduce.TaskAttemptContext.class)
            .hiddenImpl(
                "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl",
                org.apache.hadoop.conf.Configuration.class, TaskAttemptID.class)
            .hiddenImpl(
                "org.apache.hadoop.mapreduce.TaskAttemptContext",
                org.apache.hadoop.conf.Configuration.class, TaskAttemptID.class)
            .build();

    public static final DynMethods.UnboundMethod getConfiguration =
        new DynMethods.Builder("getConfiguration")
            .impl(org.apache.hadoop.mapreduce.TaskAttemptContext.class)
            .build();
  }
  public static class FSDataOutputStream {
    public static final DynMethods.UnboundMethod hflush =
        new DynMethods.Builder("hflush")
            .impl(org.apache.hadoop.fs.FSDataOutputStream.class, "hflush")
            .impl(org.apache.hadoop.fs.FSDataOutputStream.class, "sync")
            .build();

    // for CDK-203
    public static final DynMethods.UnboundMethod hsync =
        new DynMethods.Builder("hsync")
            .impl(org.apache.hadoop.fs.FSDataOutputStream.class, "hsync")
            .defaultNoop() // no hadoop-1 equivalent
            .build();
  }

  public static class SnappyCodec {
    public static final DynMethods.StaticMethod isSnappyNative =
        new DynMethods.Builder("SnappyCodec.isNativeCodeLoaded")
            .impl(org.apache.hadoop.io.compress.SnappyCodec.class,
                "isNativeCodeLoaded")
            .impl(org.apache.hadoop.io.compress.SnappyCodec.class,
                "isNativeSnappyLoaded", org.apache.hadoop.conf.Configuration.class)
            .buildStatic();
  }

  public static class Configuration {
    public static final DynMethods.UnboundMethod addResource =
      new DynMethods.Builder("addResource")
          .impl(org.apache.hadoop.conf.Configuration.class, org.apache.hadoop.conf.Configuration.class)
          .defaultNoop()
          .build();
  }
}
