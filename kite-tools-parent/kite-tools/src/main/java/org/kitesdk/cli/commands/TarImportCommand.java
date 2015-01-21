/*
 * Copyright 2014 Cloudera Inc.
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

package org.kitesdk.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.spi.DatasetRepository;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

@Parameters(commandDescription = "Import files in tarball into a Dataset")
public class TarImportCommand extends BaseDatasetCommand {

  private static final List<String> SUPPORTED_TAR_COMPRESSION_TYPES =
      Lists.newArrayList("none", "gzip", "bzip2");

  public TarImportCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<tar path> <dataset name>")
  List<String> targets;

  @Parameter(names = "--compression",
      description = "Compression type (none, gzip, bzip2)")
  String compressionType = "none";

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() == 2,
        "Tar path and target dataset name are required.");

    Path source = qualifiedPath(targets.get(0));
    FileSystem sourceFS = source.getFileSystem(getConf());
    Preconditions.checkArgument(sourceFS.exists(source),
        "Tar path does not exist: " + source);

    Preconditions.checkArgument(
        SUPPORTED_TAR_COMPRESSION_TYPES.contains(compressionType),
        "Compression type " + compressionType + " is not supported");

    String datasetName = targets.get(1);

    int success = 0;

    DatasetRepository repo = getDatasetRepository();

    Dataset<GenericRecord> tarImport = null;
    if (!repo.exists(namespace, datasetName)) {
      console.info("Creating dataset {} in repo {}", datasetName,
          repo.getUri());
      tarImport = repo.create(namespace, datasetName,
          new DatasetDescriptor.Builder()
              .schema(new Schema.Parser().parse(
                  "{\n" +
                      "  \"type\": \"record\",\n" +
                      "  \"name\": \"tarimport\",\n" +
                      "  \"fields\": [\n" +
                      "  {\n" +
                      "    \"type\": \"string\",\n" +
                      "    \"name\": \"filename\"\n" +
                      "  },\n" +
                      "  {\n" +
                      "    \"type\": \"bytes\",\n" +
                      "    \"name\": \"filecontent\"\n" +
                      "  }\n" +
                      "  ]\n" +
                      "}"
              ))
              .build()
      );
    } else {
      console
          .info("Using existing dataset {} at {}", datasetName, repo.getUri());
      tarImport = repo.load(namespace, datasetName);
    }

    DatasetWriter<GenericRecord> writer = tarImport.newWriter();

    // Create a Tar input stream wrapped in appropriate decompressor
    // Enhancement would be to use native compression libs
    TarInputStream tis = null;
    if (compressionType.equals("gzip")) {
      tis = new TarInputStream(
          new GzipCompressorInputStream(sourceFS.open(source)));
    } else if (compressionType.equals("bzip2")) {
      tis = new TarInputStream(
          new BZip2CompressorInputStream(sourceFS.open(source)));
    } else {
      tis = new TarInputStream(sourceFS.open(source));
    }
    TarEntry entry = null;
    GenericRecordBuilder builder =
        new GenericRecordBuilder(tarImport.getDescriptor().getSchema());

    try {
      int count = 0;
      boolean readError = false;
      while ((entry = tis.getNextEntry()) != null && !readError) {
        if (!entry.isDirectory()) {
          long size = entry.getSize();
          byte[] buf = new byte[(int) size];
          int read = 0;
          int br = 0;
          // TarInputStream does not always read what you ask for so loop
          // until it does. Note that this requires the entire tar entry (not
          // entire file) to fit into memory
          while (br != -1 && read != size) {
            br = tis.read(buf, read, (int) size - read);
            read += br;
          }
          if (read != size) {
            console.error("Did not read entry {} successfully " +
                    "(bytes read {}, entry size {})",
                new Object[]{entry.getName(), read, size});
            success = 1;
            readError = true;
          } else {
            writer.write(
                builder.set("filename", entry.getName())
                    .set("filecontent", buf)
                    .build()
            );
            count++;
          }
        }
      }
      console.info("Added {} records to \"{}\"", count,
          repo.getUri() + "::" + datasetName);
    } finally {
      IOUtils.closeStream(writer);
      IOUtils.closeStream(tis);
    }

    return success;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Copy the contents of from sample.tar.gz to dataset \"sample\"",
        "tar-import path/to/sample.tar.gz sample --compression gzip",
        "# Copy the records from sample.tar.bz2 to a dataset in HDFS",
        "tar-import path/to/sample.tar.bz2 sample --compression bzip2" +
            " --use-hdfs -d datasets"
    );
  }
}
