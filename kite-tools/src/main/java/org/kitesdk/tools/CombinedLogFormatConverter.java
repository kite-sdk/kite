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
package org.kitesdk.tools;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.crunch.CrunchDatasets;
import com.google.common.io.Resources;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Target;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A tool for converting files in
 * <a href="http://en.wikipedia.org/wiki/Common_Log_Format">Combined Log Format</a> to a
 * {@link org.kitesdk.data.Dataset}.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "SE_NO_SERIALVERSIONID",
    justification = "Serialization not needed between different versions of this class")
public class CombinedLogFormatConverter extends CrunchTool {

  private static final String LOG_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[" +
      "([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|-) \"([^\"]*)\" \"([^\"]+)\"";

  @Override
  public int run(String... args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: " + CombinedLogFormatConverter.class.getSimpleName() +
          " <input> <dataset_uri> <dataset name>");
      return 1;
    }
    String input = args[0];
    String datasetUri = args[1];
    String datasetName = args[2];

    Schema schema = new Schema.Parser().parse(
        Resources.getResource("combined_log_format.avsc").openStream());

    // Create the dataset
    DatasetRepository repo = DatasetRepositories.open(datasetUri);
    DatasetDescriptor datasetDescriptor = new DatasetDescriptor.Builder()
        .schema(schema).build();
    Dataset<Object> dataset = repo.create(datasetName, datasetDescriptor);

    // Run the job
    final String schemaString = schema.toString();
    AvroType<GenericData.Record> outputType = Avros.generics(schema);
    PCollection<String> lines = readTextFile(input);
    PCollection<GenericData.Record> records = lines.parallelDo(
        new ConvertFn(schemaString), outputType);
    getPipeline().write(records, CrunchDatasets.asTarget(dataset),
        Target.WriteMode.APPEND);
    run();
    return 0;
  }

  private String asString(String s) {
    if ("-".equals(s) || "\"-\"".equals(s)) {
      return null;
    }
    return s;
  }

  private Integer asInt(String s) {
    if ("-".equals(s)) {
      return null;
    }
    return Integer.parseInt(s);
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new CombinedLogFormatConverter(), args);
    System.exit(rc);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings({"SE_INNER_CLASS",
      "SE_TRANSIENT_FIELD_NOT_RESTORED"})
  private class ConvertFn extends DoFn<String, GenericData.Record> {
    private final String schemaString;
    transient Pattern pattern;
    transient GenericRecordBuilder recBuilder;

    public ConvertFn(String schemaString) {
      this.schemaString = schemaString;
    }

    public void initialize() {
      pattern = Pattern.compile(LOG_PATTERN);
      recBuilder = new GenericRecordBuilder(new Schema.Parser().parse(schemaString));
    }

    @Override
    public void process(String line, Emitter<GenericData.Record> emitter) {
      Matcher matcher = pattern.matcher(line);
      if (matcher.matches()) {
        // parse line into components
        recBuilder.set("host", asString(matcher.group(1)));
        recBuilder.set("rfc931_identity", asString(matcher.group(2)));
        recBuilder.set("username", asString(matcher.group(3)));
        recBuilder.set("datetime", asString(matcher.group(4)));
        recBuilder.set("request", asString(matcher.group(5)));
        recBuilder.set("http_status_code", asInt(matcher.group(6)));
        recBuilder.set("response_size", asInt(matcher.group(7)));
        recBuilder.set("referrer", asString(matcher.group(8)));
        recBuilder.set("user_agent", asString(matcher.group(9)));
        emitter.emit(recBuilder.build());
      } else {
        System.err.println("No match: " + line);
      }

    }
  }
}
