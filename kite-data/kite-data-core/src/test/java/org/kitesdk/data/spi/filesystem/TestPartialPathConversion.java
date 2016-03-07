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
package org.kitesdk.data.spi.filesystem;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.StorageKey;

import java.util.Collections;
import java.util.List;

public class TestPartialPathConversion {

    private static final Schema schema = SchemaBuilder.record("Event").fields()
            .requiredLong("id")
            .requiredLong("timestamp")
            .endRecord();

    private static final Path ROOT_PATH = new Path("/some/root/path");
    private static final PathConversion convert = new PartialPathConversion(ROOT_PATH, schema);
    private static final Splitter EQ = Splitter.on('=');


    @Test
    @SuppressWarnings("unchecked")
    public void testToKey() {
        PartitionStrategy strategy = new PartitionStrategy.Builder()
                .identity("id")
                .year("timestamp")
                .month("timestamp")
                .day("timestamp")
                .build();

        StorageKey expected = new StorageKey(strategy);
        expected.replaceValues((List) Lists.newArrayList(1L, 2013, 11));

        Assert.assertEquals(expected, convert.toKey(
                new Path(ROOT_PATH, "id=1/year=2013/month=11"), new StorageKey(strategy)));

        expected.replaceValues((List) Lists.newArrayList(1L, 2013));

        Assert.assertEquals(expected, convert.toKey(
                new Path(ROOT_PATH, "id=1/year=2013"), new StorageKey(strategy)));

        expected.replaceValues((List) Lists.newArrayList(1L));

        Assert.assertEquals(expected, convert.toKey(
                new Path(ROOT_PATH, "id=1"), new StorageKey(strategy)));

        expected.replaceValues(Collections.emptyList());

        Assert.assertEquals(expected, convert.toKey(
                ROOT_PATH, new StorageKey(strategy)));
    }
}
