/*
 * Copyright 2013 Cloudera.
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMultiLevelIterator {

  public static final Logger LOG = LoggerFactory.getLogger(TestMultiLevelIterator.class);

  public static class RecursiveMapIterator extends MultiLevelIterator<String> {
    private final Map<String, Map> map;

    public RecursiveMapIterator(int level, Map<String, Map> map) {
      super(level);
      this.map = map;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<String> getLevel(List<String> current) {
      Map<String, Map> currentLevel = map;
      try {
        for (String key : current) {
          currentLevel = currentLevel.get(key);
        }
      } catch (Exception ex) {
        // Should be NPE or ClassCastException
        LOG.debug("Returning empty set for {}", current);
        return Sets.newHashSet();
      }
      LOG.debug("Returning {} for {}", currentLevel.keySet(), current);
      return currentLevel.keySet();
    }
  }

  private static Map<String, Map<String, Map<String, String>>> l1;

  @BeforeClass
  public static void setup() {
    Map<String, String> l3_1 = Maps.newHashMap();
    l3_1.put("l3_1_1", "one");
    l3_1.put("l3_1_2", "two");
    Map<String, String> l3_2 = Maps.newHashMap();
    Map<String, Map<String, String>> l2_1 = Maps.newHashMap();
    l2_1.put("l2_1_1", l3_1);
    l2_1.put("l2_1_2", l3_2);

    Map<String, Map<String, String>> l2_2 = Maps.newHashMap();

    Map<String, String> l3_3 = Maps.newHashMap();
    l3_3.put("l3_3_1", "1");
    l3_3.put("l3_3_2", "2");
    Map<String, Map<String, String>> l2_3 = Maps.newHashMap();
    l2_3.put("l2_3_1", l3_3);

    l1 = Maps.newHashMap();
    l1.put("l1_1", l2_1);
    l1.put("l1_2", l2_2);
    l1.put("l1_3", l2_3);
  }

  @Test( expected=IllegalArgumentException.class )
  @SuppressWarnings("unchecked")
  public void testDepth0() {
    Iterator<List<String>> d0 = new RecursiveMapIterator(0, (Map) l1);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDepth1() {
    Iterator<List<String>> d1 = new RecursiveMapIterator(1, (Map) l1);
    Set<List<String>> expected = Sets.newHashSet(
        (List<String>) Lists.newArrayList("l1_1"),
        (List<String>) Lists.newArrayList("l1_2"),
        (List<String>) Lists.newArrayList("l1_3"));
    Assert.assertEquals(expected, Sets.newHashSet(d1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDepth2() {
    Iterator<List<String>> d2 = new RecursiveMapIterator(2, (Map) l1);
    Set<List<String>> expected = Sets.newHashSet(
        (List<String>) Lists.newArrayList("l1_1", "l2_1_1"),
        (List<String>) Lists.newArrayList("l1_1", "l2_1_2"),
        (List<String>) Lists.newArrayList("l1_3", "l2_3_1"));
    Assert.assertEquals(expected, Sets.newHashSet(d2));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDepth3() {
    Iterator<List<String>> d3 = new RecursiveMapIterator(3, (Map) l1);
    Set<List<String>> expected = Sets.newHashSet(
        (List<String>) Lists.newArrayList("l1_1", "l2_1_1", "l3_1_1"),
        (List<String>) Lists.newArrayList("l1_1", "l2_1_1", "l3_1_2"),
        (List<String>) Lists.newArrayList("l1_3", "l2_3_1", "l3_3_1"),
        (List<String>) Lists.newArrayList("l1_3", "l2_3_1", "l3_3_2"));
    Assert.assertEquals(expected, Sets.newHashSet(d3));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDepth4() {
    // at depth 4, there are only String values
    Iterator<List<String>> d4 = new RecursiveMapIterator(4, (Map) l1);
    Set<List<String>> expected = Sets.newHashSet();
    Assert.assertEquals(expected, Sets.newHashSet(d4));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDepth5() {
    // there is nothing at depth 5
    Iterator<List<String>> d5 = new RecursiveMapIterator(5, (Map) l1);
    Set<List<String>> expected = Sets.newHashSet();
    Assert.assertEquals(expected, Sets.newHashSet(d5));
  }
}
