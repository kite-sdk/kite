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
import org.kitesdk.data.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

@Parameters(commandDescription = "Import files in tarball into a Dataset")
public class TarImportCommand extends BaseDatasetCommand {

  protected enum CompressionType {
    NONE,
    GZIP,
    BZIP2
  }

  private static final List<String> SUPPORTED_TAR_COMPRESSION_TYPES =
      Lists.newArrayList("", "none", "gzip", "bzip2");

  private static final long DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;

  public TarImportCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<tar path> <dataset URI>")
  List<String> targets;

  @Parameter(names = "--compression",
      description = "Override compression type (none, gzip, bzip2)")
  String compressionType = "";

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() == 2,
        "Tar path and target dataset URI are required.");

    Preconditions.checkArgument(
        SUPPORTED_TAR_COMPRESSION_TYPES.contains(compressionType),
        "Compression type " + compressionType + " is not supported");

    String source = targets.get(0);
    String datasetUri = targets.get(1);

    long blockSize = getConf().getLong("dfs.blocksize", DEFAULT_BLOCK_SIZE);

    int success = 0;

    View<TarFileEntry> targetDataset;
    if (Datasets.exists(datasetUri)) {
      console.debug("Using existing dataset: {}", datasetUri);
      targetDataset = Datasets.load(datasetUri, TarFileEntry.class);
    } else {
      console.info("Creating new dataset: {}", datasetUri);
      DatasetDescriptor.Builder descriptorBuilder =
          new DatasetDescriptor.Builder();
      descriptorBuilder.format(Formats.AVRO);
      descriptorBuilder.schema(TarFileEntry.class);
      targetDataset = Datasets.create(datasetUri,
          descriptorBuilder.build(), TarFileEntry.class);
    }

    DatasetWriter<TarFileEntry> writer = targetDataset.newWriter();

    // Create a Tar input stream wrapped in appropriate decompressor
    // TODO: Enhancement would be to use native compression libs
    TarArchiveInputStream tis;
    CompressionType tarCompressionType = CompressionType.NONE;

    if (compressionType.isEmpty()) {
      if (source.endsWith(".tar")) {
        tarCompressionType = CompressionType.NONE;
      } else if (source.endsWith(".tar.gz")) {
        tarCompressionType = CompressionType.GZIP;
      } else if (source.endsWith(".tar.bz2")) {
        tarCompressionType = CompressionType.BZIP2;
      }
    } else if (compressionType.equals("gzip")) {
      tarCompressionType = CompressionType.GZIP;
    } else if (compressionType.equals("bzip2")) {
      tarCompressionType = CompressionType.BZIP2;
    } else {
      tarCompressionType = CompressionType.NONE;
    }

    console.info("Using {} compression", tarCompressionType);

    switch (tarCompressionType) {
      case GZIP:
        tis = new TarArchiveInputStream(
            new GzipCompressorInputStream(open(source)));
        break;
      case BZIP2:
        tis = new TarArchiveInputStream(
            new BZip2CompressorInputStream(open(source)));
        break;
      case NONE:
      default:
        tis = new TarArchiveInputStream(open(source));
    }

    TarArchiveEntry entry;

    try {
      int count = 0;
      while ((entry = tis.getNextTarEntry()) != null) {
        if (!entry.isDirectory()) {
          long size = entry.getSize();
          if (size >= blockSize) {
            console.warn("Entry \"{}\" (size {}) is larger than the " +
                "HDFS block size of {}. This may result in remote block reads",
                new Object[] { entry.getName(), size, blockSize });
          }

          byte[] buf = new byte[(int) size];
          try {
            IOUtils.readFully(tis, buf, 0, (int) size);
          } catch (IOException e) {
            console.error("Did not read entry {} successfully (entry size {})",
                entry.getName(), size);
            success = 1;
            throw e;
          }
          writer.write(
              TarFileEntry.newBuilder().setFilename(entry.getName())
                  .setFilecontent(ByteBuffer.wrap(buf)).build()
          );
          count++;
        }
      }
      console.info("Added {} records to \"{}\"", count,
              targetDataset.getDataset().getName());
    } finally {
      IOUtils.closeStream(writer);
      IOUtils.closeStream(tis);
    }

    return success;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Copy the contents of from sample.tar.gz to HDFS dataset \"sample\"",
        "path/to/sample.tar.gz dataset:hdfs:/path/to/sample"
    );
  }
}
