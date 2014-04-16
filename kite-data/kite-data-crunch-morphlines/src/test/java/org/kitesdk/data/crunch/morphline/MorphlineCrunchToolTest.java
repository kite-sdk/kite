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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.test.TemporaryPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.crunch.morphline.MorphlineCrunchToolOptions.PipelineType;
import org.kitesdk.morphline.api.MorphlineRuntimeException;

import parquet.avro.AvroParquetReader;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ObjectArrays;
import com.google.common.io.CharStreams;

@RunWith(value = Parameterized.class)
public class MorphlineCrunchToolTest extends Assert {
  
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();
 
  private String inputPath;
  private String outputPath;
  private Schema schema;
  private PipelineType pipelineType;
  private boolean isRandomizingWithDoFn;
  private boolean isDryRun;
  private WriteMode outputWriteMode;
  private boolean isPartitioned;
  private int numExpectedFailedRecords;
  private int numExpectedExceptionRecords;

  private static final String DATASET_NAME = "events";
  private static final String SCHEMA_FILE = "src/test/resources/string.avsc";
  private static final String NOP = "nop.conf";
  private static final String READ_LINE_WITH_OPEN_FILE = "readLineWithOpenFile.conf";
  private static final String READ_LINE = "readLine.conf";
  private static final String TO_AVRO = "toAvro.conf";
  private static final String EXTRACT_AVRO_PATH = "extractAvroPath.conf";  
  private static final String READ_AVRO_PARQUET_FILE = "readAvroParquetFile.conf";  
  private static final String READ_LINE_WITH_OPEN_FILE_AND_CONVERT_TO_AVRO_MAP = "readLineWithOpenFileAndConvertToAvroMap.conf";  
  private static final String FAIL_FILE = "fail.conf";  
  private static final String THROW_EXCEPTION_FILE = "throwException.conf";  
  private static final String RESOURCES_DIR = "target/test-classes";

  public MorphlineCrunchToolTest(PipelineType pipelineType, boolean isDryRun) {
    this.pipelineType = pipelineType;
    this.isDryRun = isDryRun;
  }
  
  @Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][] {
        { PipelineType.memory, false }, 
        { PipelineType.mapreduce, false },
//        { PipelineType.memory, true }, 
//        { PipelineType.mapreduce, true },
    });
  }

  @Before
  public void setUp() throws Exception {
    inputPath = tmpDir.getFileName("morphline-input");
    outputPath = tmpDir.getFileName("morphline-output");
    schema = new Schema.Parser().parse(new File(SCHEMA_FILE));   
    isRandomizingWithDoFn = false;
    outputWriteMode = WriteMode.DEFAULT;
    isPartitioned = true;
    numExpectedFailedRecords = 0;
    numExpectedExceptionRecords = 0;
  }

  private String[] getInitialArgs(String morphlineConfigFile) {
    String[] args = new String[] { 
        "--log4j=" + RESOURCES_DIR + "/log4j.properties",
        "--verbose",
        "--pipeline-type=" + pipelineType,
        "--output-write-mode=" + outputWriteMode,
    };
    if (isPartitioned) {
      args = ObjectArrays.concat(args, "--output-dataset-partition-strategy-identity-source-name=text");
      args = ObjectArrays.concat(args, "--output-dataset-partition-strategy-identity-name=ptext");
      args = ObjectArrays.concat(args, "--output-dataset-partition-strategy-identity-class=java.lang.String");
      args = ObjectArrays.concat(args, "--output-dataset-partition-strategy-identity-buckets=2");
    }
    if (morphlineConfigFile != null) {
      args = ObjectArrays.concat(args, "--morphline-file=" + RESOURCES_DIR + "/test-morphlines/" + morphlineConfigFile);
      args = ObjectArrays.concat(args, "--morphline-id=morphline1");
    }
    if (isDryRun) {
      args = ObjectArrays.concat(args, "--dry-run");      
    }
    return args;
  }

  private String[] getInputRepoArgs() {
    return new String[] {
        "--input-dataset-repository=repo:file:" + inputPath, 
        "--input-dataset-include=literal:" + DATASET_NAME,
        "--input-dataset-exclude=regex:xxxxxxxxxxx.*",
        "--input-dataset-exclude=glob:yyyyyyyyyy*",
        };
  }
  
  private String[] getOutputRepoArgs() {
    return new String[] {
        "--output-dataset-repository=repo:file:" + outputPath, 
        "--output-dataset-name=" + DATASET_NAME,
        "--output-dataset-schema=" + SCHEMA_FILE
        };
  }

  @Test
  public void testStreamTextInputFile2AvroOutputRepo() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(READ_LINE_WITH_OPEN_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    PipelineResult pipelineResult = runIntoRepo(args, expected, Formats.AVRO);
    assertTrue(pipelineResult.getStageResults().get(0).getCounterValue("morphline", "morphline.app.numRecords") > 0);
  }
  
  @Test
  public void testSplittableTextInputFile2AvroOutputRepo() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {"hello foo", "hello world"};
    isPartitioned = false;
    String[] args = getInitialArgs(TO_AVRO);
    args = ObjectArrays.concat(args, "--input-file-format=text");
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);
    if (pipelineType != PipelineType.memory) {
      runIntoRepo(args, expected, Formats.AVRO);
    } else {
      // There is a bug in Crunch :-(
      try {
        runIntoRepo(args, expected, Formats.AVRO);
        fail();
      } catch (IllegalStateException e) {
        ;
      }      
    }
  }
  
  @Test
  public void testSplittableAvroFile2AvroOutputRepo() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/strings-2.avro");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(EXTRACT_AVRO_PATH);
    args = ObjectArrays.concat(args, "--input-file-format=avro");
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    runIntoRepo(args, expected, Formats.AVRO);    
  }
  
  @Test
  public void testSplittableAvroFile2AvroOutputRepoWithDryRun() throws Exception {
    isDryRun = true;
    testSplittableAvroFile2AvroOutputRepo();
  }
  
  @Test
  public void testSplittableAvroParquetFile2AvroOutputRepo() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/strings-2.parquet");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(EXTRACT_AVRO_PATH);
    args = ObjectArrays.concat(args, "--input-file-format=avroParquet");
    args = ObjectArrays.concat(args, "--input-file-reader-schema=" + SCHEMA_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    runIntoRepo(args, expected, Formats.AVRO);    
  }
  
  @Test
  public void testStreamAvroParquetFile2AvroOutputRepo() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/strings-2.parquet");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(READ_AVRO_PARQUET_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    runIntoRepo(args, expected, Formats.AVRO);    
  }
  
  @Test
  public void testStreamTextInputFiles2AvroOutputRepo() throws Exception {
    String inputPath1 = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String inputPath2 = tmpDir.copyResourceFileName("test-documents/hello2.txt");
    
    String[] expected = new String[] {
        "hello foo", 
        "hello world",
        "hello2 file", 
        };
    String[] args = getInitialArgs(READ_LINE_WITH_OPEN_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, new String[]{inputPath1, inputPath2}, String.class);    
    runIntoRepo(args, expected, Formats.AVRO);
  }
  
  @Test
  public void testStreamTextInputFile2ParquetOutputRepo() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {"hello foo", "hello world"};
    outputWriteMode = WriteMode.OVERWRITE;
    String[] args = getInitialArgs(READ_LINE_WITH_OPEN_FILE);
    args = ObjectArrays.concat("--output-dataset-format=parquet", args);
    args = ObjectArrays.concat("--postprocessor-param=foo1:bar1", args);    
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    runIntoRepo(args, expected, Formats.PARQUET);
  }
  
  @Test
  public void testAvroInputRepo2AvroOutputRepo() throws Exception {
    runInputRepo2OutputRepo(Formats.AVRO, false);
  }
  
  @Test
  public void testParquetInputRepo2ParquetOutputRepo() throws Exception {
    runInputRepo2OutputRepo(Formats.PARQUET, false);
  }
  
  private void runInputRepo2OutputRepo(Format format, boolean isAppending) throws Exception {
    File file = new File(RESOURCES_DIR + "/test-documents/strings-2.avro");
    DatasetRepository inputRepo = DatasetRepositories.open("repo:file:" + inputPath);
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .format(format)
        .build();
    inputRepo.delete(DATASET_NAME);
    Dataset dataset = inputRepo.create(DATASET_NAME, descriptor);
    DataFileReader<GenericData.Record> reader = new DataFileReader(file, new GenericDatumReader());
    assertEquals(schema, reader.getSchema());
    DatasetWriter writer = dataset.newWriter();
    writer.open();
    while (reader.hasNext()) {
      GenericData.Record record = reader.next();
      writer.write(record);
    }
    reader.close();
    writer.flush();
    writer.close();
    
    String[] expected = new String[] {"hello world", "hello foo", "hello world", "hello foo"};
    if (isAppending) {
      expected = ObjectArrays.concat(expected, expected, String.class);      
    }
    Arrays.sort(expected);
    String[] args = getInitialArgs(EXTRACT_AVRO_PATH);
    args = ObjectArrays.concat(args, getInputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, getInputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    if (format == Formats.PARQUET) {
      args = ObjectArrays.concat(args, "--output-dataset-format=parquet");
    }
    runIntoRepo(args, expected, format);
  }
  
  @Test
  public void testStreamRecord2TextOutputFileAsMap() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(READ_LINE_WITH_OPEN_FILE_AND_CONVERT_TO_AVRO_MAP);
    args = ObjectArrays.concat(args, "--output-dir=" + outputPath);
    args = ObjectArrays.concat(args, "--output-file-format=text");
    args = ObjectArrays.concat(args, inputPath);
    runIntoRepo(args, expected, null);
  }
  
//  @Test
//  public void testStreamRecord2TextOutputFile() throws Exception {
//    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
//    String[] expected = new String[] {"hello foo", "hello world"};
//    String[] args = getInitialArgs(READ_LINE);
//    args = ObjectArrays.concat(args, "--output-dir=" + outputPath);
//    args = ObjectArrays.concat(args, "--output-file-format=text");
//    args = ObjectArrays.concat(args, inputPath);
//    runIntoRepo(args, expected, null);
//  }
  
  @Test
  public void testExistingOutputRepoWithDefaultWriteMode() throws Exception {
    outputWriteMode = WriteMode.DEFAULT;
    createEmptyOutputRepo();
    if (isDryRun) {
      return;
    }
    try {
      runInputRepo2OutputRepo(Formats.AVRO, false);
      fail();
    } catch (CrunchRuntimeException e) {
      assertTrue(e.getMessage().startsWith(
          "Refusing to proceed because output dataset " + DATASET_NAME + " already exists in repository:"));
    }
  }

  @Test
  public void testExistingOutputRepoWithOverwriteWriteMode() throws Exception {
    outputWriteMode = WriteMode.OVERWRITE;
    createEmptyOutputRepo();
    runInputRepo2OutputRepo(Formats.AVRO, false);
  }

  @Test
  public void testExistingOutputRepoWithAppendWriteMode() throws Exception {
    runInputRepo2OutputRepo(Formats.AVRO, false);
    outputWriteMode = WriteMode.APPEND;
    runInputRepo2OutputRepo(Formats.AVRO, true);
  }

  @Test
  public void testExistingOutputRepoWithCheckpointWriteMode() throws Exception {
    outputWriteMode = WriteMode.CHECKPOINT;
    createEmptyOutputRepo();
    runInputRepo2OutputRepo(Formats.AVRO, false);
  }

  private void createEmptyOutputRepo() {
    DatasetRepository repo = DatasetRepositories.open("repo:file:" + outputPath);
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .build();
    Dataset dataset = repo.create(DATASET_NAME, descriptor);
    assertNotNull(dataset);
  }
  
  @Test
  public void testStreamTextInputFile2TextOutputFile() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(READ_LINE_WITH_OPEN_FILE);
    args = ObjectArrays.concat(args, "--output-dir=" + outputPath);
    args = ObjectArrays.concat(args, "--output-file-format=text");
    args = ObjectArrays.concat(args, inputPath);
    runIntoRepo(args, expected, null);
  }
  
  @Test
  public void testFileList() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/filelist1.txt");
    String[] expected = new String[] {"hello foo", "hello world", "hello2 file"};
    String[] args = getInitialArgs(READ_LINE_WITH_OPEN_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, "--input-file-list=" + inputPath);    
    runIntoRepo(args, expected, Formats.AVRO);
  }
  
  @Test
  public void testFileListWithScheme() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/filelist1.txt");
    String[] expected = new String[] {"hello foo", "hello world", "hello2 file"};
    String[] args = getInitialArgs(READ_LINE_WITH_OPEN_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, "--input-file-list=file:" + inputPath);    
    runIntoRepo(args, expected, Formats.AVRO);
  }
  
  @Test
  public void testRecursiveInputDir() throws Exception {
    String[] expected = new String[] {"hello nadja"};
    String[] args = getInitialArgs(READ_LINE_WITH_OPEN_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, RESOURCES_DIR + "/test-documents/subdir");    
    runIntoRepo(args, expected, Formats.AVRO);
  }
  
  @Test
  public void testSplittableAvroFile2AvroOutputRepoWithoutMorphline() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/strings-2.avro");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(null);
    args = ObjectArrays.concat(args, "--input-file-format=avro");
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    runIntoRepo(args, expected, Formats.AVRO);    
  }
  
  @Test
  public void testRandomizeInputFiles() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(READ_LINE_WITH_OPEN_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);
    isRandomizingWithDoFn = true;
    runIntoRepo(args, expected, Formats.AVRO);    
  }
  
  @Test
  public void testCommandThatFails() throws Exception {
    if (pipelineType == PipelineType.memory) {
      return;
    }
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {};
    String[] args = getInitialArgs(FAIL_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    numExpectedFailedRecords = 1;
    PipelineResult pipelineResult = runIntoRepo(args, expected, Formats.AVRO);
    assertTrue(pipelineResult.getStageResults().get(0).getCounterValue("morphline", "morphline.app.numRecords") > 0);
  }
  
  @Test
  public void testCommandThatThrowsException() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {};
    String[] args = getInitialArgs(THROW_EXCEPTION_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    numExpectedExceptionRecords = 1;
    if (pipelineType != PipelineType.memory) {
      PipelineResult pipelineResult = runIntoRepo(args, expected, Formats.AVRO);
      assertTrue(pipelineResult.getStageResults().get(0).getCounterValue("morphline", "morphline.app.numRecords") > 0);
    } else {
      try {
        runIntoRepo(args, expected, Formats.AVRO);
        fail();
      } catch (MorphlineRuntimeException e) {
        ; // expected
      }
    }
  }
  
  @Test
  public void testHelp() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] args = getInitialArgs(NOP);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    args = ObjectArrays.concat(args, "--help");
    MorphlineCrunchTool tool = new MorphlineCrunchTool();
    int res = ToolRunner.run(tmpDir.getDefaultConfiguration(), tool, args);
    assertEquals(0, res);
    assertNull(tool.pipelineResult);
  }
  
  @Test
  public void testHelpWithoutArgs() throws Exception {
    String[] args = new String[0];
    MorphlineCrunchTool tool = new MorphlineCrunchTool();
    int res = ToolRunner.run(tmpDir.getDefaultConfiguration(), tool, args);
    assertEquals(0, res);
    assertNull(tool.pipelineResult);
  }
  
  @Test
  public void testIllegalCommandLineArgument() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] args = getInitialArgs(NOP);
    args = ObjectArrays.concat(args, "--illegalParam=foo");
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    MorphlineCrunchTool tool = new MorphlineCrunchTool();
    int res = ToolRunner.run(tmpDir.getDefaultConfiguration(), tool, args);
    assertEquals(1, res);
    assertNull(tool.pipelineResult);
  }
  
  @Test
  public void testIllegalCommandLineClassNameArgument() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] args = getInitialArgs(NOP);
    args = ObjectArrays.concat(args, "--input-file-format=" + ProcessBuilder.class.getName());
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    MorphlineCrunchTool tool = new MorphlineCrunchTool();
    int res = ToolRunner.run(tmpDir.getDefaultConfiguration(), tool, args);
    assertEquals(1, res);
    assertNull(tool.pipelineResult);
  }
  
  @Test
  public void testSchemaMismatchError() throws Exception {
    if (pipelineType == PipelineType.memory || isDryRun) {
      return;
    }
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] args = getInitialArgs(NOP);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, inputPath);    
    MorphlineCrunchTool tool = new MorphlineCrunchTool();
    int res = ToolRunner.run(tmpDir.getDefaultConfiguration(), tool, args);
    assertEquals(0, res);
    assertTrue(tool.pipelineResult.succeeded());
    assertEquals(1, tool.pipelineResult.getStageResults().get(0).getCounterValue("morphline", "morphline.app.numExceptionRecords"));
  }
  
  @Test
  public void testProcessorFn() throws Exception {
    String[] processorArgs = new String[] {
        "--preprocessor-class=" + PreProcessorFn.class.getName(),
        "--preprocessor-param=foo1:bar1",
        "--preprocessor-param=foo1:bar3",
        "--preprocessor-param=foo1:bar2",
        "--preprocessor-param=foo2:baz",
        "--postprocessor-class=" + PostProcessorFn.class.getName(),
    };
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(READ_LINE_WITH_OPEN_FILE);
    args = ObjectArrays.concat(args, getOutputRepoArgs(), String.class);
    args = ObjectArrays.concat(args, processorArgs, String.class);
    args = ObjectArrays.concat(args, inputPath);    
    runIntoRepo(args, expected, Formats.AVRO);
    assertTrue(PreProcessorFn.numCalls > 0);
    assertTrue(PostProcessorFn.numCalls > 0);
  }
  

  public static final class PreProcessorFn extends PipelineFn {
    
    private static int numCalls = 0;
    
    @Override
    public List<PCollection> process(List<PCollection> collections, Pipeline pipeline, List<Pair<String, String>> params) {
      numCalls++;
      List<Pair<String, String>> expected = new ArrayList();
      expected.add(Pair.of("foo1", "bar1"));
      expected.add(Pair.of("foo1", "bar3"));
      expected.add(Pair.of("foo1", "bar2"));
      expected.add(Pair.of("foo2", "baz"));
      Assert.assertEquals(expected, params);
      Assert.assertEquals(ImmutableSet.of("foo1", "foo2"), toMap(params).keySet());
      return collections;
    }
  };
  
  public static final class PostProcessorFn extends PipelineFn {      
    
    private static int numCalls = 0;
    
    @Override
    public List<PCollection> process(List<PCollection> collections, Pipeline pipeline, List<Pair<String, String>> params) {
      numCalls++;
      Assert.assertEquals(0, params.size());
      Assert.assertEquals(0, toMap(params).size());
      return collections;
    }
  };
    
  private PipelineResult runIntoRepo(String[] args, String[] expected, Format format) throws Exception {
    PipelineResult pipelineResult = runPipeline(args);    
    if (!isDryRun && pipelineType != PipelineType.memory) { // MemPipeline doesn't write output data to fs
      List<Map<String, Object>> records;
      if (format != null) {
        boolean isParquet = (format == Formats.PARQUET);
        File[] avroFiles = getDatasetFile(new File(outputPath, DATASET_NAME), isParquet ? ".parquet" : ".avro");
        records = isParquet ? readAvroParquetFile(avroFiles, schema) : readAvroFile(avroFiles, schema);
      } else {
        // interpret file as a text file containing the JSON string representation of Avro records
        // of the form {"text": "hello world"}
        File[] files = getDatasetFile(new File(outputPath), "part-m-00000");
        records = new ArrayList();
        for (File file : files) { 
          Reader reader = new InputStreamReader(new FileInputStream(file), Charsets.UTF_8);
          for (String line : CharStreams.readLines(reader)) {
            String text;
            if (line.startsWith("{message=[")) { 
              // {message=[hello world]} --> hello world
              text = line.replaceAll("\\{message=\\[(.*?)\\]\\}", "$1"); 
              System.out.println("yyyyyyyyy");     
            } else if (line.startsWith("{\"content\": {\"message\":")) { 
              // JSON from toAvroMap
              // {"content": {"message": ["hello world"]}}  --> hello world
              text = line.replaceAll(".+\\[\"?(.*?)\"?\\].+", "$1");
            } else {
              // {"text": "hello world"}  --> hello world
              text = line.replaceAll(".+\"(.*?)\"}", "$1"); 
            }
//            System.out.println("tline="+line);
//            System.out.println("ttext="+text);
            records.add(ImmutableMap.of("text", (Object) text));
          }
          reader.close();
        }
      }
      records = sort(records);
      
      //System.out.println("records = "+ records);
      assertEquals(expected.length, records.size());
      for (int i = 0; i < expected.length; i++) {
        assertEquals(ImmutableMap.of("text", expected[i]), records.get(i));
      }
    }    
    return pipelineResult;
  }
  
  private PipelineResult runPipeline(String[] args) throws Exception {
    MorphlineCrunchTool tool = new MorphlineCrunchTool();
    Configuration config = tmpDir.getDefaultConfiguration();
    config.set(MorphlineCrunchTool.MORPHLINE_VARIABLE_PARAM + ".myMorphlineVar", "foo");
    if (isRandomizingWithDoFn) {
      config.setInt(MorphlineCrunchTool.MAIN_MEMORY_RANDOMIZATION_THRESHOLD, -1); 
    }
    int res = ToolRunner.run(config, tool, args);
    assertEquals(0, res);
    assertTrue(tool.pipelineResult.succeeded());      
    assertEquals(1, tool.pipelineResult.getStageResults().size());
    StageResult stageResult = tool.pipelineResult.getStageResults().get(0);
    assertEquals(numExpectedFailedRecords, stageResult.getCounterValue("morphline", "morphline.app.numFailedRecords"));
    assertEquals(numExpectedExceptionRecords, stageResult.getCounterValue("morphline", "morphline.app.numExceptionRecords"));
    return tool.pipelineResult;
  }
  
  private File[] getDatasetFile(File dir, final String extension) {
    System.out.println("extension="+extension);
    System.out.println("files="+FileUtils.listFiles(dir, null, true));
    File[] files = Iterables.toArray(
        FileUtils.listFiles(dir, new SuffixFileFilter(extension), TrueFileFilter.INSTANCE), 
        File.class);
    if (numExpectedExceptionRecords == 0 && numExpectedFailedRecords == 0) {
      assertTrue(files.length > 0);
    }
    return files;
  }
    
  private List<Map<String, Object>> readAvroFile(File[] files, Schema schema) throws IOException {
    List<Map<String, Object>> records = new ArrayList();
    for (File file : files) {
      DataFileReader<GenericData.Record> reader = new DataFileReader(file, new GenericDatumReader());
      if (schema != null) {
        assertEquals(schema, reader.getSchema());
      }
      while (reader.hasNext()) {
        GenericData.Record record = reader.next();
        Map<String, Object> map = new HashMap();
        for (Field field : record.getSchema().getFields()) {
          map.put(field.name(), record.get(field.pos()).toString());          
        }
        records.add(map);
      }
      reader.close();
    }
    return records;    
  }
  
  private List<Map<String, Object>> readAvroParquetFile(File[] files, Schema schema) throws IOException {
    List<Map<String, Object>> records = new ArrayList();
    for (File file : files) {
      AvroParquetReader<IndexedRecord> reader = new AvroParquetReader(new Path(file.getPath()));
      while (true) {
        IndexedRecord record = reader.read();
        if (record == null) {
          break; // EOS
        }
        Map<String, Object> map = new HashMap();
        for (Field field : record.getSchema().getFields()) {
          map.put(field.name(), record.get(field.pos()).toString());          
        }
        records.add(map);
      }
      reader.close();
    }
    return records;    
  }
  
  private List<Map<String, Object>> sort(List<Map<String, Object>> records) {
    Collections.sort(records, new Comparator<Map>() {

      @Override
      public int compare(Map o1, Map o2) {
        Comparable c1 = Iterables.toArray(o1.values(), Comparable.class)[0];
        Comparable c2 = Iterables.toArray(o2.values(), Comparable.class)[0];
        return c1.compareTo(c2);
      }
      
    });
    return records;    
  }    
}

//List<String> lines = Files.readLines(files[0], Charsets.UTF_8);
//boolean passed = false;
//for (String line : lines) {
//System.out.println("xxxxxxxxxxxxxx="+ line);
////if (line.startsWith("Macbeth\t28") || line.startsWith("[Macbeth,28]")) {
////  passed = true;
////  break;
////}
//}
////assertTrue(passed);
