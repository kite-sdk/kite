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
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;

public class TestURIBuilder {
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

  private static final String ID = UUID.randomUUID().toString();

  private static final Constraints empty = new Constraints(SCHEMA, STRATEGY);

  @Test
  public void testRepoUriAndNameToDatasetUri() {
    Assert.assertEquals("Should construct the correct dataset URI",
        URI.create("dataset:file:/datasets/test-name"),
        new URIBuilder("repo:file:/datasets", "test-name").build());
  }

  @Test
  public void testRepoUriAndNameConstructorRejectsBadUris() {
    TestHelpers.assertThrows("Should reject dataset: URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("dataset:file:/datasets/test-name", "test-name-2")
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject view: URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("view:file:/datasets/test-name?n=34", "test-name-2")
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject null URI",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder((String) null, "test-name").build();
          }
        });
    TestHelpers.assertThrows("Should reject null URI",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder((URI) null, "test-name").build();
          }
        });
    TestHelpers.assertThrows("Should reject null name",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("repo:file:/datasets", null).build();
          }
        });
  }

  @Test
  public void testRepoUriAndNameToDatasetUriPreservesOptions() {
    Assert.assertEquals("Should construct the correct dataset URI",
        URI.create("dataset:file:/datasets/test-name?hdfs:port=1080"),
        new URIBuilder("repo:file:/datasets?hdfs:port=1080", "test-name")
            .build());
  }

  @Test
  public void testRepoUriAndNameAddEquals() {
    Assert.assertEquals("Should construct the correct dataset URI",
        URI.create("view:file:/datasets/test-name?prop=value"),
        new URIBuilder("repo:file:/datasets", "test-name")
            .with("prop", "value")
            .build());
    // order should be preserved
    Assert.assertEquals("Should construct the correct dataset URI",
        URI.create("view:file:/datasets/test-name?prop=value&num=34"),
        new URIBuilder("repo:file:/datasets", "test-name")
            .with("prop", "value")
            .with("num", 34)
            .build());
  }

  @Test
  public void testDatasetUriToDatasetUri() {
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("dataset:file:/datasets/test-name"),
        new URIBuilder("dataset:file:/datasets/test-name").build());
  }

  @Test
  public void testDatasetUriConstructorRejectsBadUris() {
    TestHelpers.assertThrows("Should reject repo: URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("repo:file:/datasets/test-name").build();
          }
        });
    TestHelpers.assertThrows("Should reject null URI",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder((String) null).build();
          }
        });
    TestHelpers.assertThrows("Should reject null URI",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder((URI) null).build();
          }
        });
  }

  @Test
  public void testDatasetUriToDatasetUriPreservesOptions() {
    // this doesn't produce a view URI because the original isn't a view URI
    Assert.assertEquals("Should construct the correct dataset URI",
        URI.create("dataset:file:/datasets/test-name?hdfs:port=1080"),
        new URIBuilder("dataset:file:/datasets/test-name?hdfs:port=1080")
            .build());
  }

  @Test
  public void testDatasetUriAddEquals() {
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("view:file:/datasets/test-name?prop=value"),
        new URIBuilder("dataset:file:/datasets/test-name")
            .with("prop", "value")
            .build());
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("view:file:/datasets/test-name?prop=value&num=34"),
        new URIBuilder("dataset:file:/datasets/test-name")
            .with("prop", "value")
            .with("num", 34)
            .build());
  }

  @Test
  public void testViewUriToViewUri() {
    Assert.assertEquals("Should produce an equivalent view URI",
        URI.create("view:file:/datasets/test-name?prop=value"),
        new URIBuilder("view:file:/datasets/test-name?prop=value").build());
  }

  @Test
  public void testViewUriAddEquals() {
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("view:file:/datasets/test-name?prop=value&field=v2"),
        new URIBuilder("view:file:/datasets/test-name?prop=value")
            .with("field", "v2")
            .build());
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("view:file:/datasets/test-name?prop=value&field=v2&num=34"),
        new URIBuilder("view:file:/datasets/test-name?prop=value")
            .with("field", "v2")
            .with("num", 34)
            .build());
  }

  @Test
  public void testAddEqualityConstraints() {
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:file:/datasets/test?id=" + ID),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.with("id", new Utf8(ID)))
                .build());
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:file:/datasets/test?id=a,b"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.with("id", new Utf8("a"), new Utf8("b")))
            .build());
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:file:/datasets/test?id=" + ID + "&timestamp=1405720705333"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(
                empty.with("id", new Utf8(ID)).with("timestamp", 1405720705333L))
            .build());
  }

  @Test
  public void testAddExistsConstraints() {
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:file:/datasets/test?id=exists()"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.with("id"))
            .build());
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:file:/datasets/test?id=exists()&timestamp=exists()"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.with("id").with("timestamp"))
            .build());
  }

  @Test
  public void testAddRangeConstraints() {
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:file:/datasets/test?color=[green,)"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.from("color", "green"))
            .build());
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:file:/datasets/test?color=(,green]"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.to("color", "green"))
            .build());
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:file:/datasets/test?timestamp=[0,1405720705333)&color=(green,red]"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty
                .from("timestamp", 0l).toBefore("timestamp", 1405720705333L)
                .fromAfter("color", "green").to("color", "red"))
            .build());
  }

  @Test
  public void testEmptyConstraints() {
    Assert.assertEquals("Empty constraints should produce dataset URI",
        URI.create("dataset:file:/datasets/test"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty)
            .build());
  }
}
