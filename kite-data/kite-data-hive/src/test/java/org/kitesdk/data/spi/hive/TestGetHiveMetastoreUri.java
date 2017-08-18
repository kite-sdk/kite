/*
 * Copyright 2017 Cloudera Inc.
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

package org.kitesdk.data.spi.hive;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.kitesdk.data.spi.MetadataProvider;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestGetHiveMetastoreUri {

  @Parameters
  public static Iterable<? extends Object> metastoreUriParameters() {
    return Arrays.asList(new Object[] {null, null},
        new Object[] {"", ""},
        new Object[] {"thrift://host-1:9083", "thrift://host-1:9083"},
        new Object[] {"thrift://host-1:9083,thrift://host-2:9083", "thrift://host-1:9083"},
        new Object[] {"thrift://host-1:9083,thrift://host-2:9083", "thrift://host-1:9083"},
        new Object[] {"thrift://host-1:9083,thrift://host-2:9083,thrift://host-3:9083", "thrift://host-1:9083"});
  }

  private final String input;

  private final String expected;

  private HiveAbstractDatasetRepository repository;

  public TestGetHiveMetastoreUri(String input, String expected) {
    this.input = input;
    this.expected = expected;
  }

  @Before
  public void before() {
    Configuration configuration = new Configuration();
    MetadataProvider metadataProvider = new HiveManagedMetadataProvider(configuration);

    repository = new HiveAbstractDatasetRepository(configuration, metadataProvider);
  }

  @Test
  public void testGetHiveMetastoreUri() {
    Configuration configuration = new Configuration();
    if (input != null) {
      configuration.set(Loader.HIVE_METASTORE_URI_PROP, input);
    } else {
      configuration.unset(Loader.HIVE_METASTORE_URI_PROP);
    }

    assertEquals(expected, repository.getHiveMetastoreUri(configuration));
  }
}
