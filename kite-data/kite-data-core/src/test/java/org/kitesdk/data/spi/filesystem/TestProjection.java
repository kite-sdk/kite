/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi.filesystem;

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.net.URI;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.event.IncompatibleEvent;
import org.kitesdk.data.event.ReflectSmallEvent;
import org.kitesdk.data.event.ReflectStandardEvent;
import org.kitesdk.data.event.SmallEvent;
import org.kitesdk.data.event.StandardEvent;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.TestRefinableViews;

public class TestProjection extends TestRefinableViews {

  public TestProjection(boolean distributed) {
    super(distributed);
  }

  @Override
  public DatasetRepository newRepo() {
    return new FileSystemDatasetRepository.Builder()
        .configuration(conf)
        .rootDirectory(URI.create("target/data"))
        .build();
  }

  @After
  public void removeDataPath() throws IOException {
    fs.delete(new Path("target/data"), true);
  }

  @Test
  public void testSpecificProjection() throws IOException {
    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = unbounded.newWriter();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }

    Dataset<SmallEvent> dataset = repo.load(
        "ns", unbounded.getDataset().getName(),
        SmallEvent.class);

    Set<SmallEvent> expected = Sets.newHashSet(toSmallEvent(sepEvent),
        toSmallEvent(octEvent), toSmallEvent(novEvent));

    assertContentEquals(expected, dataset);
  }

  @Test
  public void testReflectProjection() throws IOException {
    Dataset<ReflectStandardEvent> original = repo.create(
        "ns", "reflectProjection",
        new DatasetDescriptor.Builder()
            .schema(ReflectStandardEvent.class)
            .build(), ReflectStandardEvent.class);

    DatasetWriter<ReflectStandardEvent> writer = null;
    try {
      writer = original.newWriter();
      writer.write(new ReflectStandardEvent(sepEvent));
      writer.write(new ReflectStandardEvent(octEvent));
      writer.write(new ReflectStandardEvent(novEvent));
    } finally {
      Closeables.close(writer, false);
    }

    Dataset<ReflectSmallEvent> dataset = repo.load("ns", original.getName(),
        ReflectSmallEvent.class);

    Set<ReflectSmallEvent> expected = Sets.newHashSet(
        new ReflectSmallEvent(sepEvent), new ReflectSmallEvent(octEvent),
        new ReflectSmallEvent(novEvent));

    assertContentEquals(expected, dataset);
  }

  @Test
  public void testMixedProjection() throws IOException {
    Dataset<StandardEvent> original = repo.create("ns", "mixedProjection",
        new DatasetDescriptor.Builder()
            .schema(StandardEvent.class)
            .build(), StandardEvent.class);

    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = original.newWriter();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }

    Dataset<ReflectSmallEvent> dataset = repo.load("ns", original.getName(),
        ReflectSmallEvent.class);

    Set<ReflectSmallEvent> expected = Sets.newHashSet(
        new ReflectSmallEvent(sepEvent), new ReflectSmallEvent(octEvent),
        new ReflectSmallEvent(novEvent));

    assertContentEquals(expected, dataset);
  }

  @Test(expected=IncompatibleSchemaException.class)
  public void testIncompatibleProjection() throws IOException {
    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = unbounded.newWriter();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }

    repo.load("ns", unbounded.getDataset().getName(), IncompatibleEvent.class);
  }

  private static SmallEvent toSmallEvent(StandardEvent event) {
    return SmallEvent.newBuilder()
        .setUserId(event.getUserId())
        .setSessionId(event.getSessionId())
        .build();
  }

  private static IncompatibleEvent toIncompatibleEvent(StandardEvent event) {
    return IncompatibleEvent.newBuilder()
        .setUserId(String.valueOf(event.getUserId()))
        .setSessionId(event.getSessionId())
        .build();
  }
}
