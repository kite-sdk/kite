/*
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
package org.kitesdk.morphline.metrics.scalable;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

public class ScalableStatisticsTest extends Assert {

  @Test
  public void testBasic() throws Exception {
    double maxDelta = 0.00000000001;
    assertEquals(0, new ScalableStatistics().getCount());
    
    for (int i = 0; i < 2; i++) {
      int exactQuantilesThreshold = i == 0 ? 3 : 10000;
      ScalableStatistics stats = new ScalableStatistics(exactQuantilesThreshold);
      assertEquals(Double.NaN, stats.getQuantile(0.5), maxDelta);
      try {
        stats.getQuantile(1.1);
        fail();
      } catch (IllegalArgumentException e) {
        ; // expected
      }
      
      try {
        stats.getQuantile(-0.1);
        fail();
      } catch (IllegalArgumentException e) {
        ; // expected
      }
      stats.copy();      
      stats.add(0);
      assertEquals(0.0, stats.getQuantile(1.0), maxDelta);
      stats.copy();
      
      ScalableStatistics stats3 = new ScalableStatistics(exactQuantilesThreshold);
      stats3.add(-10);
      stats3.add(70);
      stats3.add(stats);
      //System.out.println(stats3);
      String expected3 = "count:3, min:-10.0, max:70.0, mean:20.0, sum:60.0, stddev:35.59026084010437, median:0.0, p75:35.0, p95:62.99999999999999, p98:67.2, p99:68.6, p999:69.86";
      assertEquals(expected3, stats3.toString());
      
      double max = 40.0;
      stats.add(max);
      assertEquals(max, stats.getQuantile(1.0), maxDelta);
      stats.copy();      
      stats.add(30);
      assertEquals(max, stats.getQuantile(1.0), maxDelta);
      stats.add(20);
      assertEquals(max, stats.getQuantile(1.0), maxDelta);
      stats.add(10);
      assertEquals(max, stats.getQuantile(1.0), maxDelta);
      assertEquals(5, stats.getCount());
      //System.out.println("iter="+i + ": " + stats.toString());
      String expected = "count:5, min:0.0, max:40.0, mean:20.0, sum:100.0, stddev:14.142135623730951, median:20.0, p75:30.0, p95:38.0, p98:39.2, p99:39.6, p999:39.96";
      assertEquals(expected, stats.toString());
      ScalableStatistics stats2 = stats.copy();
      assertEquals(expected, stats2.toString());
      stats.add(stats2);
      assertEquals(10, stats.getCount());
      //System.out.println(stats.toString());
      expected = "count:10, min:0.0, max:40.0, mean:20.0, sum:200.0, stddev:14.142135623730951, median:20.0, p75:30.0, p95:40.0, p98:40.0, p99:40.0, p999:40.0";
      assertEquals(expected, stats.toString());
    }
  }
  
  @Test
  public void testNISTExample() {
    double[] values = new double[] { 95.1772, 95.1567, 95.1937, 95.1959, 95.1442, 95.0610, 95.1591, 95.1195, 95.1772,
        95.0925, 95.1990, 95.1682 };
    for (int i = 0; i < 2; i++) {
      int exactQuantilesThreshold = i == 0 ? 3 : 10000;
      ScalableStatistics stats = new ScalableStatistics(exactQuantilesThreshold);
      for (double value : values) {
        stats.add(value);
      }
      assertEquals(95.19568, stats.getQuantile(0.9), 0);
      assertEquals(95.1990, stats.getQuantile(0.999), 1.0e-4);
      assertEquals(95.1990, stats.getQuantile(1.0), 0);
    }
  }

  @Test
  public void testMinMaxWithNegativeValue() throws Exception {
    double maxDelta = 0.00000000001;
    ScalableStatistics stats = new ScalableStatistics();
    stats.add(-5);
    assertEquals(-5, stats.getMin(), maxDelta);
    assertEquals(-5, stats.getMax(), maxDelta);
  }
  
  @Test
  public void testAccuracy() throws Exception {
    testAccuracyInternal(0.99, 10,     0.0, null);
    testAccuracyInternal(0.99, 1000,   0.000000000000001, null);
    testAccuracyInternal(0.99, 10000,  0.0001, null);
    
    testAccuracyInternal(0.90, 10,     0.000000000000001, null);
    testAccuracyInternal(0.90, 1000,   0.001, null);
    testAccuracyInternal(0.90, 10000,  0.01, null);
    
    // same params but with custom tdigest with higher accuracy:
    testAccuracyInternal(0.99, 10,     0.0, createCustomTDigest());
    testAccuracyInternal(0.99, 1000,   0.000000000000001, createCustomTDigest());
    testAccuracyInternal(0.99, 10000,  0.000000000000001, createCustomTDigest());
    
    testAccuracyInternal(0.90, 10,     0.000000000000001, createCustomTDigest());
    testAccuracyInternal(0.90, 1000,   0.000000000000001, createCustomTDigest());
    testAccuracyInternal(0.90, 10000,  0.001, createCustomTDigest());
  }
  
  private TDigest createCustomTDigest() {
    return new AVLTreeDigest(400);
  }

  private void testAccuracyInternal(double quantile, int size, double maxDelta, TDigest tdigest) {
    Random rand = new Random(12345);
    ScalableStatistics approx = 
        tdigest == null ? 
            new ScalableStatistics(0) : 
            new ScalableStatistics(0, tdigest);
    ScalableStatistics exact  = new ScalableStatistics(size + 3);
    for (int i = 0; i < size; i++) {
      double value = rand.nextDouble();
      exact.add(value);
      approx.add(value);
      assertEquals(exact.getQuantile(quantile), approx.getQuantile(quantile), maxDelta);
    }
  }

}
