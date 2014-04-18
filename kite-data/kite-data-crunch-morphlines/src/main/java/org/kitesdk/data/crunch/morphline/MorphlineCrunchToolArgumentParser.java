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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.action.HelpArgumentAction;
import net.sourceforge.argparse4j.impl.choice.RangeArgumentChoice;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.FeatureControl;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.crunch.Pair;
import org.apache.crunch.Target.WriteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.crunch.morphline.MorphlineCrunchToolOptions.InputDatasetSpec;
import org.kitesdk.data.crunch.morphline.MorphlineCrunchToolOptions.PipelineType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parquet.avro.AvroParquetInputFormat;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;


/**
 * See http://argparse4j.sourceforge.net and for details see http://argparse4j.sourceforge.net/usage.html
 */
final class MorphlineCrunchToolArgumentParser {

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineCrunchToolArgumentParser.class);
  
  private static final Map<String, String> INPUT_FORMAT_SUBSTITUTIONS = ImmutableMap.of(
      "text", TextInputFormat.class.getName(),
      "avro", AvroKeyInputFormat.class.getName(),
      "avroParquet", AvroParquetInputFormat.class.getName()
      );

  private static final Map<String, String> OUTPUT_FORMAT_SUBSTITUTIONS = ImmutableMap.of(
      "text", TextOutputFormat.class.getName()
      );

  /**
   * Parses the given command line arguments.
   *
   * @return exitCode null indicates the caller shall proceed with processing,
   *         non-null indicates the caller shall exit the program with the
   *         given exit status code.
   */
  public Integer parseArgs(String[] args, Configuration conf, MorphlineCrunchToolOptions opts) {
    assert args != null;
    assert conf != null;
    assert opts != null;

    if (args.length == 0) {
      args = new String[] { "--help" };
    }

    ArgumentParser parser = ArgumentParsers
        .newArgumentParser("hadoop [GenericOptions]... jar kite-data-crunch-morphlines-*-job.jar " + MorphlineCrunchTool.class.getName(), false)
        .defaultHelp(true)
        .description(
              "MapReduce ETL batch job driver that pipes data from an input source (splitable or non-splitable HDFS files "
            + "or Kite datasets) to an output target (HDFS files or Kite dataset or Apache Solr), and along the way runs the "
            + "data through a (optional) Morphline for extraction and transformation, followed by a (optional) Crunch join "
            + "followed by a (optional) arbitrary custom sequence of Crunch processing steps. The program is designed for "
            + "flexible, scalable and fault-tolerant batch ETL pipeline jobs. It is implemented as an Apache Crunch pipeline "
            + "and as such can run on either the Apache Hadoop MapReduce or Apache Spark execution engine.\n"
            + "\n"
            + "The program proceeds in several consecutive phases, as follows: "
            + "\n\n"
            + "1) Randomization phase: This (parallel) phase randomizes the list of HDFS input files in order to spread "
            + "ingestion load more evenly among the mapper tasks of the subsequent phase. This phase is only executed for "
            + "non-splitables files, and skipped otherwise."
            + "\n\n"
            + "2) Extraction phase: This (parallel) phase emits a series of HDFS input file paths (for non-splitable files) "
            + "or a series of input data records (for splitable files or Kite datasets), in the form of a Crunch PCollection. "
            + "\n\n"
            + "3) Preprocessing phase: This optional (parallel) phase takes a configurable custom Java class to arbitrarily "
            + "transform a Crunch PCollection (the output of the previous phase) into another Crunch PCollection. "
            + "\n\n"
            + "4) Morphline phase: This optional (parallel) phase takes the items of the PCollection generated by the previous "
            + "phase, and uses a Morphline to extract the relevant content, transform it and pipe zero or more Avro objects "
            + "into another PCollection. The ETL functionality is flexible and customizable using chains of arbitrary "
            + "morphline commands that pipe records from one transformation command to another. Commands to parse and "
            + "transform a set of standard data formats such as Avro, Parquet, CSV, Text, HTML, XML, PDF, MS-Office, etc. "
            + "are provided out of the box, and additional custom commands and parsers for additional file or data formats "
            + "can be added as custom morphline commands or custom Hadoop Input Formats. Any kind of data format can be "
            + "processed and any kind output format can be generated by any custom Morphline ETL logic. Also, this phase "
            + "can be used to send data directly to a live SolrCloud cluster (via the loadSolr morphline command) and "
            + "subsequently emit zero items to the next phase (via the dropRecord morphline command)."
            + "\n\n"
            + "5) Join & Postprocessing phase: This optional (parallel) phase takes a configurable custom Java class to "
            + "arbitrarily transform a Crunch PCollection (the output of the previous phase) into another Crunch PCollection. "
            + "For example, this phase can join multiple data sources and afterwards send data directly to a live SolrCloud "
            + "cluster (via the loadSolr command used as part of a second morphline - see class MorphlineFn which is a "
            + "Crunch DoFn) and subsequently emit zero items to the next phase."
            + "\n\n"
            + "6) Output phase: This (parallel) phase loads the output of the previous phase into HDFS files or a Kite Dataset."
            + "\n\n"
            + "The program is implemented as a Crunch pipeline and as such Crunch optimizes the logical phases mentioned "
            + "above into an efficient physical execution plan that runs a single mapper-only job (e.g. if no pre and "
            + "postprocessing phase is specified) or a complex sequence of many consecutive MapReduce jobs (if complex pre or "
            + "postprocessing joins, sorting or groupBys are specified), or as the corresponding Spark equivalent."
            + "\n\n"
            + "Fault Tolerance: Task attempts are retried on failure per the standard MapReduce or Spark "
            + "semantics. On program startup all data in --output-dir or --output-dataset-name is deleted if that output "
            + "location already exists and the --output-write-mode=OVERWRITE parameter is specified. If the whole job fails "
            + "you can retry simply by rerunning the program again using the same arguments."
        );

    ArgumentGroup inputDatasetArgGroup = parser.addArgumentGroup("Input dataset arguments (also see http://ow.ly/uSn4q)");

    final List<InputDatasetSpec> inputDatasetSpecs = new ArrayList();
    
    inputDatasetArgGroup.addArgument("--input-dataset-repository")
        .metavar("REPOSITORY_URI")
        .type(String.class)
        .help("Kite Dataset Repository URI to read from. Syntax is repo:[storage component], where the "
            + "[storage component] indicates the underlying metadata and, in some cases, physical storage "
            + "of the data, along with any options. Examples: file:[path] or hdfs://[host]:[port]/[path] or "
            + "hive://[metastore-host]:[metastore-port]/ or hive://[metastore-host]:[metastore-port]/[path] "
            + "or hbase:[zookeeper-host1]:[zk-port],[zookeeper-host2],... as described in detail here: "
            + "http://ow.ly/uSczs"
            + "\n\n"
            + "Multiple --input-dataset-repository arguments can be specified, with the --input-dataset-* "
            + "options described below always applying to the most recently specified --input-dataset-repository. "
            + "For example, to ingest repoA/datasetA1 as well as repoA/datasetA2 as well as repoB/datasetB "
            + "specify the following: \n"
            + "--input-dataset-repository repo:hdfs://localhost:/repoA1 \n"
            + "--input-dataset-include literal:datasetA1 \n"
            + "--input-dataset-include literal:datasetA2 \n"
            + "--input-dataset-repository repo:hdfs://localhost:/repoB \n"
            + "--input-dataset-include literal:datasetB")
        .action(new ArgumentAction() {

          @Override
          public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value)
              throws ArgumentParserException {
            
            inputDatasetSpecs.add(new InputDatasetSpec(value.toString()));
          }
  
          @Override
          public boolean consumeArgument() { return true; }
          
          @Override
          public void onAttach(Argument arg) {}
        });
    
    inputDatasetArgGroup.addArgument("--input-dataset-include")
        .metavar("STRING")
        .type(String.class)
        .help("An expression to match against the Kite Dataset names contained in the most recently specified "
            + "--input-dataset-repository. A dataset that matches any --input-dataset-include expression yet "
            + "does not match any --input-dataset-exclude expression will be ingested.\n"
            + "The expression can be "
            + "a literal string equality match (example: 'literal:events'), or a regex match (example: "
            + "'regex:logs-v[234]-california-.*') or a glob match (example: 'glob:logs-*-california-*'). "
            + "Default is to match all dataset names ('glob:*' aka '*'), i.e. include everything. "
            + "Multiple --input-dataset-include arguments can be specified.")
        .action(new ArgumentAction() {
  
          @Override
          public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value)
              throws ArgumentParserException {
            
            if (inputDatasetSpecs.isEmpty()) {
              throw new ArgumentParserException(
                  "You must specify --input-dataset-repository prior to --input-dataset-include", parser);
            }
            InputDatasetSpec current = inputDatasetSpecs.get(inputDatasetSpecs.size() - 1);
            current.addInclude(value.toString());
          }

          @Override
          public boolean consumeArgument() { return true; }
          
          @Override
          public void onAttach(Argument arg) {}
        });

    inputDatasetArgGroup.addArgument("--input-dataset-exclude")
        .metavar("STRING")
        .type(String.class)
        .help("An expression to match against the Kite Dataset names contained in the most recently specified "
            + "--input-dataset-repository. A dataset that matches any --input-dataset-include expression yet "
            + "does not match any --input-dataset-exclude expression will be ingested.\n"
            + "The expression can be "
            + "a literal string equality match (example: 'literal:events'), or a regex match (example: "
            + "'regex:logs-v[234]-california.*') or a glob match (example: 'glob:logs-*-california-*'). "
            + "Default is to match no dataset names, i.e. exclude nothing. "
            + "Multiple --input-dataset-exclude arguments can be specified.")
        .action(new ArgumentAction() {
  
          @Override
          public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value)
              throws ArgumentParserException {
            
            if (inputDatasetSpecs.isEmpty()) {
              throw new ArgumentParserException(
                  "You must specify --input-dataset-repository prior to --input-dataset-exclude", parser);
            }
            InputDatasetSpec current = inputDatasetSpecs.get(inputDatasetSpecs.size() - 1);
            current.addExclude(value.toString());
          }
  
          @Override
          public boolean consumeArgument() { return true; }
          
          @Override
          public void onAttach(Argument arg) {}
        });

    ArgumentGroup inputFileArgGroup = parser.addArgumentGroup("Input file arguments");
    
    // trailing positional arguments
    Argument inputFilesArg = inputFileArgGroup.addArgument("input-files")
        .metavar("HDFS_URI")
        .type(new PathArgumentType(conf).verifyExists().verifyCanRead())
        .nargs("*")
        .setDefault()
        .help("HDFS URI of file or directory tree to ingest (unless --input-dataset-repository is specified).");

    Argument inputFileListArg = inputFileArgGroup.addArgument("--input-file-list")
        .action(Arguments.append())
        .metavar("URI")
        .type(new PathArgumentType(conf).acceptSystemIn().verifyExists().verifyCanRead())
        .help("Local URI or HDFS URI of a UTF-8 encoded file containing a list of HDFS URIs to ingest, " +
            "one URI per line in the file. If '-' is specified, URIs are read from the standard input. " +
            "Multiple --input-file-list arguments can be specified.");

    Argument inputFormatArg = inputFileArgGroup.addArgument("--input-file-format")
        .metavar("FQCN")
        .type(String.class)
        .help("The Hadoop FileInputFormat to use for extracting data from splitable HDFS files. Can be a "
            + "fully qualified Java class name or one of ['text', 'avro', 'avroParquet']. If this option "
            + "is present the extraction phase will emit a series of input data records rather than a series "
            + "of HDFS file paths (unless --input-dataset-repo is given, in which case the extraction phase "
            + "will emit Avro records from the (splitable) Kite dataset).");

    Argument inputFileProjectionSchemaArg = inputFileArgGroup.addArgument("--input-file-projection-schema")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .help("Relative or absolute path to an Avro schema file on the local file system. This will be used "
            + "as the projection schema for Parquet input files (but not yet for Kite Parquet input datasets).");

    ArgumentGroup inputDatasetAndFileArgGroup = parser.addArgumentGroup("Input dataset and file arguments");
    
    Argument inputFileReaderSchemaArg = inputDatasetAndFileArgGroup.addArgument("--input-file-reader-schema")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .help("Relative or absolute path to an Avro schema file on the local file system. This will be used "
            + "as the reader schema for Avro or Parquet input files or Kite input datasets. "
            + "Example: src/test/resources/strings.avsc");

    ArgumentGroup outputDatasetArgGroup = parser.addArgumentGroup("Output dataset arguments");

    Argument outputDatasetRepoArg = outputDatasetArgGroup.addArgument("--output-dataset-repository")
        .metavar("REPOSITORY_URI")
        .type(String.class)
        .help("Kite Dataset Repository URI to write output to. For the syntax see the documentation above "
            + "for the --input-dataset-repository option.");

    Argument outputDatasetNameArg = outputDatasetArgGroup.addArgument("--output-dataset-name")
        .metavar("STRING")
        .type(String.class)
        .help("The name of the Kite Dataset to write output to inside of the --output-dataset-repository. "
            + "Example: 'events'.");

    Argument outputDatasetSchemaArg = outputDatasetArgGroup.addArgument("--output-dataset-schema")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .help("Relative or absolute path to an Avro schema file on the local file system. This will be used "
            + "on output as the writer schema for Kite datasets. Example: src/test/resources/strings.avsc");

    Argument outputDatasetFormatArg = outputDatasetArgGroup.addArgument("--output-dataset-format")
        .metavar("STRING")
        .type(String.class)
        .setDefault(Formats.AVRO.getName())
        .help("The type of format to use on output for Kite datasets. Can be one of ['avro', 'parquet'].");

    ArgumentGroup outputDatasetPartitioningArgGroup = parser.addArgumentGroup(
        "Output dataset partitioning arguments (also see http://ow.ly/uSchn)");
    
    final List<IdentityFieldPartitionerSpec> fieldPartitionSpecs = new ArrayList();

    outputDatasetPartitioningArgGroup.addArgument("--output-dataset-partition-strategy-identity-source-name")
        .metavar("STRING")
        .type(String.class)
        .help("The entity field name from which to get values to be partitioned. Multiple "
            + "--output-dataset-partition-strategy-identity-source-name arguments can be specified. "
            + "Example that partitions along two dimensions, in that order: \n"
            + "--output-dataset-partition-strategy-identity-source-name fieldA1 \n"
            + "--output-dataset-partition-strategy-identity-source fieldA2 \n"
            + "--output-dataset-partition-strategy-identity-class " + String.class.getName() + "\n"
            + "--output-dataset-partition-strategy-identity-buckets 100 \n"
            + "--output-dataset-partition-strategy-identity-source-name fieldB1 \n"
            + "--output-dataset-partition-strategy-identity-source fieldB2 \n"
            + "--output-dataset-partition-strategy-identity-class " + Integer.class.getName() + "\n"
            + "--output-dataset-partition-strategy-identity-buckets 100")
        .action(new ArgumentAction() {
    
          @Override
          public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value)
              throws ArgumentParserException {
            
            fieldPartitionSpecs.add(new IdentityFieldPartitionerSpec(value.toString()));
          }
    
          @Override
          public boolean consumeArgument() { return true; }
          
          @Override
          public void onAttach(Argument arg) {}
        });

    outputDatasetPartitioningArgGroup.addArgument("--output-dataset-partition-strategy-identity-name")
        .metavar("STRING")
        .type(String.class)
        .help("A name for the partition field of the most recently specified partitioner.")
        .action(new ArgumentAction() {
    
          @Override
          public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value)
              throws ArgumentParserException {
            
            if (fieldPartitionSpecs.isEmpty()) {
              throw new ArgumentParserException(
                  "You must specify --output-dataset-partition-strategy-identity-source-name prior to --output-dataset-partition-strategy-identity-name", parser);
            }
            IdentityFieldPartitionerSpec current = fieldPartitionSpecs.get(fieldPartitionSpecs.size() - 1);
            current.setName(value.toString());
          }
    
          @Override
          public boolean consumeArgument() { return true; }
          
          @Override
          public void onAttach(Argument arg) {}
        });

    outputDatasetPartitioningArgGroup.addArgument("--output-dataset-partition-strategy-identity-class")
        .metavar("FQCN")
        .type(String.class)
        .setDefault(String.class.getName())
        .help("The type of the partition field of the most recently specified partitioner. This must match the schema.")
        .action(new ArgumentAction() {
    
          @Override
          public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value)
              throws ArgumentParserException {
            
            if (fieldPartitionSpecs.isEmpty()) {
              throw new ArgumentParserException(
                  "You must specify --output-dataset-partition-strategy-identity-source-name prior to --output-dataset-partition-strategy-identity-class", parser);
            }
            IdentityFieldPartitionerSpec current = fieldPartitionSpecs.get(fieldPartitionSpecs.size() - 1);
            Class type;
            try {
              type = Class.forName(value.toString());
            } catch (ClassNotFoundException e) {
              throw new RuntimeException(e);
            }
            current.setType(type);
          }

          @Override
          public boolean consumeArgument() { return true; }
          
          @Override
          public void onAttach(Argument arg) {}
        });
    
    outputDatasetPartitioningArgGroup.addArgument("--output-dataset-partition-strategy-identity-buckets")
        .metavar("INTEGER")
        .type(Integer.class)
        .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
        .setDefault(1)
        .help("A hint as to the number of partitions that will be created for the most recently "
            + "specified partitioner (i.e. the number of discrete values for the "
            + "--output-dataset-partition-strategy-identity-name field in the data.")
        .action(new ArgumentAction() {
    
          @Override
          public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value)
              throws ArgumentParserException {
            
            if (fieldPartitionSpecs.isEmpty()) {
              throw new ArgumentParserException(
                  "You must specify --output-dataset-partition-strategy-identity-source-name prior to --output-dataset-partition-strategy-identity-buckets", parser);
            }
            IdentityFieldPartitionerSpec current = fieldPartitionSpecs.get(fieldPartitionSpecs.size() - 1);
            current.setBuckets(Integer.parseInt(value.toString()));
          }
    
          @Override
          public boolean consumeArgument() { return true; }
          
          @Override
          public void onAttach(Argument arg) {}
        });
    
    ArgumentGroup outputFileArgGroup = parser.addArgumentGroup("Output file arguments");
    
    Argument outputDirArg = outputFileArgGroup.addArgument("--output-dir")
        .metavar("HDFS_URI")
        .type(new PathArgumentType(conf).verifyIsAbsolute().verifyCanWriteParent())
        .help("The path of an HDFS directory to write output to (unless --output-dataset-repository is "
            + "specified). Example: hdfs://localhost:/events");
      
    Argument outputFileFormatArg = outputFileArgGroup.addArgument("--output-file-format")
        .metavar("FQCN")
        .type(String.class)
        .help("The Hadoop FileOutputFormat to use on output for HDFS files "
            + "(unless --output-dataset-repository is specified). Can be a fully qualified Java class "
            + "name or one of ['text']. Example: 'text'");

    ArgumentGroup outputDatasetAndFileArgGroup = parser.addArgumentGroup("Output dataset and file arguments");
    
    Argument outputWriteModeArg = outputDatasetAndFileArgGroup.addArgument("--output-write-mode")
        .metavar("STRING")
        .type(WriteMode.class)
        .setDefault(WriteMode.DEFAULT)
        .help("Indicates different options the client may specify for handling the case where the output "
            + "path, dataset, etc. referenced by a target already exists. Can be one of "
            + "['DEFAULT', 'OVERWRITE', 'APPEND', 'CHECKPOINT']. For details see "
            + "http://crunch.apache.org/apidocs/0.9.0/org/apache/crunch/Target.WriteMode.html");

    ArgumentGroup morphlineArgGroup = parser.addArgumentGroup("Morphline phase (also see http://ow.ly/uSnsv)");
    
    Argument morphlineFileArg = morphlineArgGroup.addArgument("--morphline-file")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .help("Relative or absolute path to a local config file that contains one or more morphlines. "
            + "The file must be UTF-8 encoded. It will be uploaded to each MR task. "
            + "Example: /path/to/morphline.conf");

    Argument morphlineIdArg = morphlineArgGroup.addArgument("--morphline-id")
        .metavar("STRING")
        .type(String.class)
        .help("The identifier of the morphline that shall be executed within the morphline config file "
            + "specified by --morphline-file. If the --morphline-id option is ommitted the first (i.e. "
            + "top-most) morphline within the config file is used. Example: morphline1");

    ArgumentGroup preAndPostProcessingArgGroup = parser.addArgumentGroup(
        "Pre and Postprocessing phase (also see http://crunch.apache.org/user-guide.html)");
    
    Argument preProcessorClassArg = preAndPostProcessingArgGroup.addArgument("--preprocessor-class")
        .metavar("FQCN")
        .type(String.class)
        .help("The fully qualified name of a Java class that implements the " + PipelineFn.class.getName() + 
            " interface. This class will be called in the preprocessing phase to transform a Crunch PCollection "
            + "into another PCollection. Java jars containing such custom code can be submitted via the "
            + "--libjars options. Example: com.mycompany.test.MyPipelineFn");

    Argument preProcessorParamArg = preAndPostProcessingArgGroup.addArgument("--preprocessor-param")
        .action(Arguments.append())
        .metavar("STRING:STRING")
        .type(new PairArgumentType())
        .help("A name:value pair that will be fed into the custom preprocessor PipelineFn. "
            + "Multiple --preprocessor-param arguments can be specified. "
            + "Example: --preprocessor-param ignoreErrors:false "
            + "--preprocessor-param myJoinTable:hdfs:localhost:/myJoinTable.csv");

    Argument postProcessorClassArg = preAndPostProcessingArgGroup.addArgument("--postprocessor-class")
        .metavar("FQCN")
        .type(String.class)
        .help("The fully qualified name of a Java class that implements the " + PipelineFn.class.getName() + 
            " interface. This class will be called in the postprocessing phase to transform a Crunch PCollection "
            + "into another PCollection. Java jars containing such custom code can be submitted via the "
            + "--libjars options. Example: com.mycompany.test.MyPipelineFn");

    Argument postProcessorParamArg = preAndPostProcessingArgGroup.addArgument("--postprocessor-param")
        .action(Arguments.append())
        .metavar("STRING:STRING")
        .type(new PairArgumentType())
        .help("A name:value pair that will be fed into the custom postprocessor PipelineFn. "
            + "Multiple --postprocessor-param arguments can be specified. "
            + "Example: --postprocessor-param ignoreErrors:false "
            + "--postprocessor-param myJoinTable:hdfs:localhost:/myJoinTable.csv");

    ArgumentGroup optionalGroup = parser.addArgumentGroup("Misc arguments");

    optionalGroup.addArgument("--help", "-help", "-h")
        .help("Show this help message and exit")
        .action(new HelpArgumentAction() {
          @Override
          public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value) throws ArgumentParserException {
            parser.printHelp();
            System.out.println();
            System.out.print(ToolRunnerHelpFormatter.getGenericCommandUsage());
            //ToolRunner.printGenericCommandUsage(System.out);
            System.out.println(
                "Examples: \n\n" 
                    + "# Ingest text file line by line into a Kite Avro Dataset\n"
                    + "# and on the way transform each line with a morphline:\n"
                    + "hadoop fs -copyFromLocal src/test/resources/test-documents/hello1.txt hdfs:/user/systest/input/\n"
                    + "hadoop \\\n"
                    + "  --config /etc/hadoop/conf.cloudera.YARN-1 \\\n"
                    + "  jar target/kite-data-crunch-morphlines-*-job.jar " + MorphlineCrunchTool.class.getName() + " \\\n"
                    + "  -D 'mapred.child.java.opts=-Xmx500m' \\\n"
                    + "  --files src/test/resources/string.avsc \\\n"
                    + "  --morphline-file src/test/resources/test-morphlines/readLineWithOpenFileCWD.conf \\\n"
                    + "  --output-dataset-repository repo:hdfs:/user/systest/data1 \\\n"
                    + "  --output-dataset-name events \\\n"
                    + "  --output-dataset-schema src/test/resources/string.avsc \\\n"
                    + "  --output-dataset-format avro \\\n"
                    + "  --output-write-mode OVERWRITE \\\n"
                    + "  --log4j src/test/resources/log4j.properties \\\n"
                    + "  --verbose \\\n"
                    + "  hdfs:/user/systest/input/hello1.txt\n"
                    + "\n"
                    + "# Replace readLineWithOpenFileCWD.conf with\n"
                    + "# readSplittableLines.conf in the command above,\n"
                    + "# then add this to say input files are splitable, rerun:\n"
                    + "# --input-file-format=text\n"
                    + "\n"
                    + "# View the output of the job:\n"
                    + "hadoop fs -copyToLocal '/user/systest/data1/events/*.avro' /tmp/\n"
                    + "wget http://archive.apache.org/dist/avro/avro-1.7.6/java/avro-tools-1.7.6.jar\n"
                    + "java -jar avro-tools-1.7.6.jar getschema /tmp/*.avro\n"
                    + "java -jar avro-tools-1.7.6.jar tojson /tmp/*.avro\n"
                    + "\n"
                    + "# The above command will display this:\n"
                    + "# {\"text\":\"hello world\"}\n"
                    + "# {\"text\":\"hello foo\"}\n"
                    + "\n"
                    + "# Ingest from a Kite Dataset into a Kite Dataset\n"
                    + "# and on the way transform data with a morphline:\n"
                    + "hadoop \\\n"
                    + "  --config /etc/hadoop/conf.cloudera.YARN-1 \\\n"
                    + "  jar target/kite-data-crunch-morphlines-*-job.jar " + MorphlineCrunchTool.class.getName() + " \\\n"
                    + "  -D 'mapred.child.java.opts=-Xmx500m' \\\n"
                    + "  --files src/test/resources/string.avsc \\\n"
                    + "  --input-dataset-repository repo:hdfs:/user/systest/data1 \\\n"
                    + "  --morphline-file src/test/resources/test-morphlines/extractAvroPathCWD.conf \\\n"
                    + "  --output-dataset-repository repo:hdfs:/user/systest/data2 \\\n"
                    + "  --output-dataset-name events \\\n"
                    + "  --output-dataset-schema src/test/resources/string.avsc \\\n"
                    + "  --output-dataset-format avro \\\n"
                    + "  --output-write-mode OVERWRITE \\\n"
                    + "  --log4j src/test/resources/log4j.properties \\\n"
                    + "  --verbose"
            );
            throw new FoundHelpArgument(); // Trick to prevent processing of any remaining arguments
          }
        });

    Argument mappersArg = optionalGroup.addArgument("--mappers")
        .metavar("INTEGER")
        .type(Integer.class)
        .choices(new RangeArgumentChoice(-1, Integer.MAX_VALUE)) // TODO: also support X% syntax where X is an integer
        .setDefault(-1)
        .help("Tuning knob that indicates the maximum number of MR mapper tasks to use. -1 indicates use all map slots " +
            "available on the cluster. This parameter only applies to non-splitable input files.");

    Argument pipelineTypeArg = optionalGroup.addArgument("--pipeline-type")
        .metavar("STRING")
        .type(PipelineType.class)
        .setDefault(PipelineType.mapreduce)
        .help(FeatureControl.SUPPRESS);

    Argument dryRunArg = optionalGroup.addArgument("--dry-run")
        .action(Arguments.storeTrue())
        .help("Run the pipeline but do not write output to the target specified by --output-dataset-repository "
            + "or --output-dir. This can be used for quicker turnaround during early trial & debug sessions.");

    Argument log4jConfigFileArg = optionalGroup.addArgument("--log4j")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .help("Relative or absolute path to a log4j.properties config file on the local file system. This file " +
            "will be uploaded to each MR task. Example: /path/to/log4j.properties");

    Argument verboseArg = optionalGroup.addArgument("--verbose", "-v")
        .action(Arguments.storeTrue())
        .help("Turn on verbose output.");

    Namespace ns;
    try {
      ns = parser.parseArgs(args);
    } catch (FoundHelpArgument e) {
      return 0;
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      return 1;
    }

    opts.log4jConfigFile = (File) ns.get(log4jConfigFileArg.getDest());
    if (opts.log4jConfigFile != null) {
      PropertyConfigurator.configure(opts.log4jConfigFile.getPath());
    }
    LOG.debug("Parsed command line args: {}", ns);

    opts.inputFileLists = getList(ns, inputFileListArg);
    opts.inputFiles = ns.get(inputFilesArg.getDest());
    opts.inputDatasetSpecs = inputDatasetSpecs;
    opts.outputDatasetRepository = ns.get(outputDatasetRepoArg.getDest());
    opts.outputDatasetName = ns.get(outputDatasetNameArg.getDest());
    opts.outputDatasetFormat = Formats.fromString(ns.getString(outputDatasetFormatArg.getDest()));
    opts.outputWriteMode = ns.get(outputWriteModeArg.getDest());
    opts.outputDir = ns.get(outputDirArg.getDest());
    opts.mappers = (Integer) ns.get(mappersArg.getDest());
    opts.morphlineFile = ns.get(morphlineFileArg.getDest());
    opts.morphlineId = ns.get(morphlineIdArg.getDest());
    opts.pipelineType = ns.get(pipelineTypeArg.getDest());
    opts.preProcessorParams = getList(ns, preProcessorParamArg);
    opts.postProcessorParams = getList(ns, postProcessorParamArg);
    opts.isDryRun = (Boolean) ns.get(dryRunArg.getDest());
    opts.isVerbose = (Boolean) ns.get(verboseArg.getDest());

    try {
      opts.inputFileReaderSchema = parseSchema((File)ns.get(inputFileReaderSchemaArg.getDest()), parser);
      opts.inputFileProjectionSchema = parseSchema((File)ns.get(inputFileProjectionSchemaArg.getDest()), parser);
      opts.outputDatasetSchema = parseSchema((File)ns.get(outputDatasetSchemaArg.getDest()), parser);
      opts.inputFileFormat = getClass(inputFormatArg, ns, FileInputFormat.class, parser, INPUT_FORMAT_SUBSTITUTIONS);
      opts.outputFileFormat = getClass(outputFileFormatArg, ns, FileOutputFormat.class, parser, OUTPUT_FORMAT_SUBSTITUTIONS);
      opts.preProcessor = newInstance(preProcessorClassArg, ns, PipelineFn.class, parser);
      opts.postProcessor = newInstance(postProcessorClassArg, ns, PipelineFn.class, parser);
      if (fieldPartitionSpecs.size() > 0) {
        PartitionStrategy.Builder builder = new PartitionStrategy.Builder();
        for (IdentityFieldPartitionerSpec fieldPartitionSpec : fieldPartitionSpecs) {
          builder = fieldPartitionSpec.apply(builder, parser);
        }
        opts.outputDatasetPartitionStrategy = builder.build();
      }      
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      return 1;
    }

    return null;
  }
  
  private <T> T getList(Namespace ns, Argument arg) {
    T list = ns.get(arg.getDest());
    if (list == null) {
      list = (T) new ArrayList();
    }
    return list;
  }
  
  private Schema parseSchema(File file, ArgumentParser parser) throws ArgumentParserException {
    if (file == null) {
      return null;
    }
    try {
      return new Schema.Parser().parse(file);
    } catch (IOException e) {
      throw new ArgumentParserException(e, parser);
    }
  }
  
  private Class getClass(Argument arg, Namespace ns, Class baseClazz, ArgumentParser parser)
      throws ArgumentParserException {
    
    return getClass(arg, ns, baseClazz, parser, null);
  }
  
  private Class getClass(Argument arg, Namespace ns, Class baseClazz, ArgumentParser parser,
      Map<String, String> substitutions) throws ArgumentParserException {
    
    Class clazz = null;
    String className = ns.getString(arg.getDest());
    if (substitutions != null && substitutions.containsKey(className)) {
      className = substitutions.get(className);
      Preconditions.checkNotNull(className);
    }
    if (className != null) {
      try {
        clazz = Class.forName(className);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      if (!baseClazz.isAssignableFrom(clazz)) {
        throw new ArgumentParserException("--" + arg.getDest().replace('_', '-') + " " + className
            + " must be an instance of class " + baseClazz.getName(), parser);
      }
    }
    return clazz;
  }
  
  private <T> T newInstance(Argument arg, Namespace ns, Class baseClazz, ArgumentParser parser)
      throws ArgumentParserException {
    
    Class clazz = getClass(arg, ns, baseClazz, parser);
    if (clazz == null) {
      return null;
    }
    try {
      return (T) clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }      
  }

  /** Marker trick to prevent processing of any remaining arguments once --help option has been parsed */
  private static final class FoundHelpArgument extends RuntimeException {
  }
  

  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class IdentityFieldPartitionerSpec {
    
    private final String sourceName; 
    private String name = null;
    private Class type = String.class;
    private int buckets = 1;
    
    public IdentityFieldPartitionerSpec(String sourceName) {
      Preconditions.checkNotNull(sourceName);
      this.sourceName = sourceName;
    }
    
    public void setName(String name) {
      this.name = name;
    }
    
    public void setType(Class type) {
      this.type = type;
    }
    
    public void setBuckets(int buckets) {
      this.buckets = buckets;
    }
    
    public PartitionStrategy.Builder apply(PartitionStrategy.Builder builder, ArgumentParser parser) 
        throws ArgumentParserException {
      
      if (name == null) {
        throw new ArgumentParserException(
            "Missing --output-dataset-partition-strategy-identity-name parameter", parser);
      }
      return builder.identity(sourceName, name, type, buckets);
    }
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * ArgumentType subclass for Name:Value type, using fluent style API.
   */
  private static final class PairArgumentType implements ArgumentType<Pair<String, String>> {
    
    public PairArgumentType() {}
    
    @Override
    public Pair<String, String> convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
      int i = value.indexOf(':');
      if (i < 0) {
        throw new ArgumentParserException("Illegal syntax for name:value pair: " + value, parser);
      }    
      return new Pair(value.substring(0, i), value.substring(i + 1));
    }
  }
      
}