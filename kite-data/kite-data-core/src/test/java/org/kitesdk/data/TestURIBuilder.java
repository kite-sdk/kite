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

package org.kitesdk.data;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import java.net.URI;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.spi.Constraints;

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
    assertEquivalent("Should construct the correct dataset URI",
        URI.create("dataset:file:/datasets/ns/test-name"),
        new URIBuilder("repo:file:/datasets", "ns", "test-name").build());
  }

  @Test
  public void testRepoUriAndNameConstructorRejectsBadUris() {
    TestHelpers.assertThrows("Should reject dataset: URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("dataset:file:/datasets/test-name", "ns", "test-name-2")
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject view: URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("view:file:/datasets/test-name?n=34", "ns", "test-name-2")
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject null URI",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder((String) null, "ns", "test-name").build();
          }
        });
    TestHelpers.assertThrows("Should reject null URI",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder((URI) null, "ns", "test-name").build();
          }
        });
    TestHelpers.assertThrows("Should reject null namespace",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("repo:file:/datasets", null, "test-name").build();
          }
        });
    TestHelpers.assertThrows("Should reject null name",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("repo:file:/datasets", "ns", null).build();
          }
        });
  }

  @Test
  public void testRepoUriAndNameToDatasetUriPreservesOptions() {
    assertEquivalent("Should construct the correct dataset URI",
        URI.create("dataset:file:/datasets/ns/test-name?hdfs:port=1080"),
        new URIBuilder("repo:file:/datasets?hdfs:port=1080", "ns", "test-name")
            .build());
  }

  @Test
  public void testRepoUriAndNameAddEquals() {
    assertEquivalent("Should construct the correct dataset URI",
        URI.create("view:file:/datasets/ns/test-name?prop=value"),
        new URIBuilder("repo:file:/datasets", "ns", "test-name")
            .with("prop", "value")
            .build());
    // order should be preserved
    assertEquivalent("Should construct the correct dataset URI",
        URI.create("view:file:/datasets/ns/test-name?prop=value&num=34"),
        new URIBuilder("repo:file:/datasets", "ns", "test-name")
            .with("prop", "value")
            .with("num", 34)
            .build());
  }

  @Test
  public void testDatasetUriToDatasetUri() {
    assertEquivalent("Should produce an equivalent dataset URI",
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
    assertEquivalent("Should construct the correct dataset URI",
        URI.create("dataset:file:/datasets/test-name?hdfs:port=1080"),
        new URIBuilder("dataset:file:/datasets/test-name?hdfs:port=1080")
            .build());
  }

  @Test
  public void testDatasetUriAddEquals() {
    assertEquivalent("Should produce an equivalent dataset URI",
        URI.create("view:file:/datasets/test-name?prop=value"),
        new URIBuilder("dataset:file:/datasets/test-name")
            .with("prop", "value")
            .build());
    assertEquivalent("Should produce an equivalent dataset URI",
        URI.create("view:file:/datasets/test-name?prop=value&num=34"),
        new URIBuilder("dataset:file:/datasets/test-name")
            .with("prop", "value")
            .with("num", 34)
            .build());
  }

  @Test
  public void testViewUriToViewUri() {
    assertEquivalent("Should produce an equivalent view URI",
        URI.create("view:file:/datasets/test-name?prop=value"),
        new URIBuilder("view:file:/datasets/test-name?prop=value").build());
  }

  @Test
  public void testViewUriAddEquals() {
    assertEquivalent("Should produce an equivalent dataset URI",
        URI.create("view:file:/datasets/test-name?prop=value&field=v2"),
        new URIBuilder("view:file:/datasets/test-name?prop=value")
            .with("field", "v2")
            .build());
    assertEquivalent("Should produce an equivalent dataset URI",
        URI.create("view:file:/datasets/test-name?prop=value&field=v2&num=34"),
        new URIBuilder("view:file:/datasets/test-name?prop=value")
            .with("field", "v2")
            .with("num", 34)
            .build());
  }

  @Test
  public void testAddEqualityConstraints() {
    assertEquivalent("Should add equality constraints",
        URI.create("view:file:/datasets/test?id=" + ID),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.with("id", new Utf8(ID)))
            .build());
    assertEquivalent("Should add equality constraints",
        URI.create("view:file:/datasets/test?id=a,b"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.with("id", new Utf8("a"), new Utf8("b")))
            .build());
    assertEquivalent("Should add equality constraints",
        URI.create("view:file:/datasets/test?id=" + ID + "&timestamp=1405720705333"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(
                empty.with("id", new Utf8(ID)).with("timestamp", 1405720705333L))
            .build());
  }

  @Test
  public void testAddExistsConstraints() {
    assertEquivalent("Should add equality constraints",
        URI.create("view:file:/datasets/test?id="),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.with("id"))
            .build());
    assertEquivalent("Should add equality constraints",
        URI.create("view:file:/datasets/test?id=&timestamp="),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.with("id").with("timestamp"))
            .build());
  }

  @Test
  public void testAddRangeConstraints() {
    assertEquivalent("Should add equality constraints",
        URI.create("view:file:/datasets/test?color=[green,)"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.from("color", "green"))
            .build());
    assertEquivalent("Should add equality constraints",
        URI.create("view:file:/datasets/test?color=(,green]"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty.to("color", "green"))
            .build());
    assertEquivalent("Should add equality constraints",
        URI.create("view:file:/datasets/test?timestamp=[0,1405720705333)&color=(green,red]"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty
                .from("timestamp", 0l).toBefore("timestamp", 1405720705333L)
                .fromAfter("color", "green").to("color", "red"))
            .build());
  }

  @Test
  public void testEmptyConstraints() {
    assertEquivalent("Empty constraints should produce dataset URI",
        URI.create("dataset:file:/datasets/test"),
        new URIBuilder("dataset:file:/datasets/test")
            .constraints(empty)
            .build());
  }

  public static void assertEquivalent(String message, URI expected, URI actual) {
    Assert.assertEquals("URI scheme does not match (" + message + ")",
        expected.getScheme(), actual.getScheme());
    Assert.assertEquals("URI userInfo does not match (" + message + ")",
        expected.getUserInfo(), actual.getUserInfo());
    Assert.assertEquals("URI authority does not match (" + message + ")",
        expected.getAuthority(), actual.getAuthority());
    Assert.assertEquals("URI path does not match (" + message + ")",
        expected.getPath(), actual.getPath());
    Assert.assertEquals("URI fragment does not match (" + message + ")",
        expected.getFragment(), actual.getFragment());
    Assert.assertEquals("URI query does not match (" + message + ")",
        set(expected.getQuery()), set(actual.getQuery()));
  }

  public static Multiset<String> set(@Nullable String ampSeparatedValues) {
    if (ampSeparatedValues == null) {
      return null;
    }
    return ImmutableMultiset.copyOf(Splitter.on('&').split(ampSeparatedValues));
  }
}
