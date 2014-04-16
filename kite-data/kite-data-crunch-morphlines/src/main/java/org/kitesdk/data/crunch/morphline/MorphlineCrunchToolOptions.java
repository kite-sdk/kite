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
package org.kitesdk.data.crunch.morphline;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.crunch.Pair;
import org.apache.crunch.Target.WriteMode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.kitesdk.data.Format;
import org.kitesdk.data.PartitionStrategy;

import com.google.common.base.Preconditions;


/**
 * Parsed command line options. These are produced by {@link MorphlineCrunchToolArgumentParser} and
 * consumed by {@link MorphlineCrunchTool}.
 */
final class MorphlineCrunchToolOptions {

  List<Path> inputFileLists;
  List<Path> inputFiles;
  Class<FileInputFormat> inputFileFormat; // e.g. TextInputFormat
  Schema inputFileReaderSchema;
  Schema inputFileProjectionSchema;
  List<InputDatasetSpec> inputDatasetSpecs;
  Path outputDir;
  Class<FileOutputFormat<Writable, Writable>> outputFileFormat; // e.g. TextOutputFormat
  String outputDatasetRepository;
  String outputDatasetName;
  Schema outputDatasetSchema;
  Format outputDatasetFormat;
  PartitionStrategy outputDatasetPartitionStrategy;
  WriteMode outputWriteMode;
  File morphlineFile;
  String morphlineId;
  PipelineType pipelineType;
  String sparkConnection;
  PipelineFn preProcessor;
  List<Pair<String, String>> preProcessorParams;
  PipelineFn postProcessor;
  List<Pair<String, String>> postProcessorParams;
  boolean isDryRun;
  File log4jConfigFile;
  boolean isVerbose;
  int mappers;

  public MorphlineCrunchToolOptions() {}
    
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  static enum PipelineType {
    memory,
    mapreduce,
    spark
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  static final class InputDatasetSpec {
    
    private final String repositoryUri;
    private final List<String> includes = new ArrayList<String>();
    private final List<String> excludes = new ArrayList<String>();
    
    public InputDatasetSpec(String repositoryUri) {
      Preconditions.checkNotNull(repositoryUri);
      this.repositoryUri = repositoryUri;
    }
    
    public String getRepositoryUri() {
      return repositoryUri;
    }
  
    public List<String> getIncludes() {
      return includes;
    }
  
    public void addInclude(String include) {
      Preconditions.checkNotNull(include);
      includes.add(include);
    }
    
    public List<String> getExcludes() {
      return excludes;
    }
  
    public void addExclude(String exclude) {
      Preconditions.checkNotNull(exclude);
      excludes.add(exclude);
    }
      
  }

}
