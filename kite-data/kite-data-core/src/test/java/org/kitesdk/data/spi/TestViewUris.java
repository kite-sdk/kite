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

package org.kitesdk.data.spi;

import java.net.URI;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.View;

public class TestViewUris {
  private static final Schema SCHEMA = SchemaBuilder.record("Event").fields()
      .requiredString("id")
      .requiredLong("timestamp")
      .requiredString("color")
      .endRecord();

  private static final PartitionStrategy STRATEGY = new PartitionStrategy.Builder()
      .hash("id", "id-hash", 64)
      .year("timestamp")
      .month("timestamp")
      .day("timestamp")
      .identity("id")
      .build();

  private static Dataset<GenericRecord> test;

  @BeforeClass
  public static void createTestDataset() {
    Datasets.delete("dataset:file:/tmp/test_name");
    test = Datasets.create("dataset:file:/tmp/test_name",
        new DatasetDescriptor.Builder()
            .schema(SCHEMA)
            .partitionStrategy(STRATEGY)
            .build());
  }

  @Test
  public void testSimpleViews() {
    assertViewUriEquivalent("dataset",
        "dataset:file:/tmp/test_name", test);
    assertViewUriEquivalent("to constraint",
        "view:file:/tmp/test_name?timestamp=(,0]",
        test.to("timestamp", 0L));
    assertViewUriEquivalent("View with toBefore constraint",
        "view:file:/tmp/test_name?timestamp=(,0)",
        test.toBefore("timestamp", 0L));
    assertViewUriEquivalent("View with from constraint",
        "view:file:/tmp/test_name?timestamp=[0,)",
        test.from("timestamp", 0L));
    assertViewUriEquivalent("View with fromAfter constraint",
        "view:file:/tmp/test_name?timestamp=(0,)",
        test.fromAfter("timestamp", 0L));
    assertViewUriEquivalent("View with in(\"\") constraint",
        "view:file:/tmp/test_name?color=in()",
        test.with("color", ""));
    assertViewUriEquivalent("View with in constraint",
        "view:file:/tmp/test_name?color=orange,red",
        test.with("color", "orange", "red"));
    assertViewUriEquivalent("View with exists constraint",
        "view:file:/tmp/test_name?id=",
        test.with("id"));
  }

  @Test
  public void testBoundedRangeViews() {
    assertViewUriEquivalent("[a,b]",
        "view:file:/tmp/test_name?id=[a,b]",
        test.from("id", "a").to("id", "b"));
    assertViewUriEquivalent("[a,b)",
        "view:file:/tmp/test_name?id=[a,b)",
        test.from("id", "a").toBefore("id", "b"));
    assertViewUriEquivalent("(a,b]",
        "view:file:/tmp/test_name?id=(a,b]",
        test.fromAfter("id", "a").to("id", "b"));
    assertViewUriEquivalent("(a,b)",
        "view:file:/tmp/test_name?id=(a,b)",
        test.fromAfter("id", "a").toBefore("id", "b"));
  }

  @Test
  public void testMixedConstraintViews() {
    assertViewUriEquivalent("id, color, and time constraints",
        "view:file:/tmp/test_name?color=,orange&id=exists()&timestamp=[0,9)",
        test.with("color", "", "orange").with("id")
            .from("timestamp", 0L).toBefore("timestamp", 9L));
  }

  @Test
  public void testConstraintWithEncodedCharacters() {
    assertViewUriEquivalent("encoded constraints",
        "view:file:/tmp/test_name?color=a%2Fb",
        test.with("color", "a/b"));
  }

  public void assertViewUriEquivalent(String desc, String uri,
                                      View<GenericRecord> view) {
    View<GenericRecord> loaded = Datasets.load(uri);
    Assert.assertEquals("URI should produce the correct View (" + desc + ")",
        view, loaded);
    URI loadedUri = loaded.getUri();
    View<GenericRecord> reloaded = Datasets.load(loadedUri);
    Assert.assertEquals("Loaded URI should also load correctly (" + desc + ")",
        view, reloaded);
    Assert.assertEquals("URI should be consistent after load (" + desc + ")",
        loadedUri, reloaded.getUri());
  }
}
