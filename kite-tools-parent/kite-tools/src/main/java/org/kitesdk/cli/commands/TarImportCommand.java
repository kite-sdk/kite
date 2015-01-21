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
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.io.IOUtils;
import org.kitesdk.cli.commands.tarimport.avro.TarFileEntry;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.View;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
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

    Preconditions.checkArgument(
        SUPPORTED_TAR_COMPRESSION_TYPES.contains(compressionType),
        "Compression type " + compressionType + " is not supported");

    String source = targets.get(0);
    String datasetName = targets.get(1);

    int success = 0;

    View<TarFileEntry> target = load(datasetName, TarFileEntry.class);

    DatasetWriter<TarFileEntry> writer = target.newWriter();

    // Create a Tar input stream wrapped in appropriate decompressor
    // Enhancement would be to use native compression libs
    TarArchiveInputStream tis = null;

    if (compressionType.equals("gzip")) {
      tis = new TarArchiveInputStream(
          new GzipCompressorInputStream(open(source)));
    } else if (compressionType.equals("bzip2")) {
      tis = new TarArchiveInputStream(
          new BZip2CompressorInputStream(open(source)));
    } else {
      tis = new TarArchiveInputStream(open(source));
    }
    TarArchiveEntry entry = null;

    try {
      int count = 0;
      boolean readError = false;
      while ((entry = tis.getNextTarEntry()) != null && !readError) {
        if (!entry.isDirectory()) {
          long size = entry.getSize();
          byte[] buf = new byte[(int) size];
          int read = 0;
          int br = 0;
          // Loop until entire entry read. Note that this requires the entire
          // tar entry contents to fit into memory
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
                TarFileEntry.newBuilder().setFilename(entry.getName())
                    .setFilecontent(ByteBuffer.wrap(buf)).build()
            );
            count++;
          }
        }
      }
      console.info("Added {} records to \"{}\"", count,
          target.getDataset().getName());
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
