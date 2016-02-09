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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.tdunning.math.stats.AVLTreeDigest;
//import com.tdunning.math.stats.ArrayDigest;
//import com.tdunning.math.stats.MergingDigest;
//import com.tdunning.math.stats.TreeDigest;
import com.tdunning.math.stats.TDigest;

/**
 * Given an arbitrarily large dataset, this class maintains descriptive statistics in O(1) memory,
 * and also maintains approximate quantiles (e.g. percentiles) requiring little memory per
 * https://github.com/tdunning/t-digest
 * 
 * For small datasets a data structure and algorithm is used that computes exact quantiles (without
 * approximation). After N values have been inserted we switch to the approximate algorithm.
 * 
 * The class can be serialized and deserialized and multiple instances can be merged, e.g. the
 * independent stats and quantiles of each separate MapReduce task can be merged into a single
 * overall job summary statistic.
 */
public final class ScalableStatistics {
  
  // for exact basic descriptive stats:
  private long count = 0; 
  private double min = Double.MAX_VALUE;
  private double max = -Double.MAX_VALUE;
  private double sum = 0;
  private double sumOfSquares = 0;
    
  // for approximate quantiles:
  private TDigest tdigest;

  // for exact quantiles:
  private int exactQuantilesThreshold;
  private double[] exactValues;
  private int numExactValues = 0;
  private boolean areExactValuesSorted = false;
  
  private static final int EXACT_QUANTILES_THRESHOLD = 100;
  private static final int COMPRESSION = 100;
  private static final int MAGIC_CODE = 384979084; // for sanity checks of data format
  private static final int MAX_DEFAULT_ARRAY_LENGTH = 0;
  
  /** Constructs an instance with default parameters */
  public ScalableStatistics() {
    this(EXACT_QUANTILES_THRESHOLD);
  }
  
  ScalableStatistics(int exactQuantilesThreshold) {
    this(exactQuantilesThreshold, getDefaultTDigest());
  }
  
  /**
   * Constructs an instance with given parameters.
   * 
   * @param exactQuantilesThreshold
   *          Determines after how many added values to dynamically switch the algorithm from exact
   *          quantiles to approximate quantiles. Must be greater or equal to zero. Set this
   *          parameter to zero if you want to always maintain approximate quantiles. In practise,
   *          the approximation error is more noticeable on small data sets than on big data sets, so to
   *          guarantee high quality quantiles even for small datasets you might want to set this
   *          parameter to something like 100, which requires about 800 Bytes RAM, proportional to
   *          exactQuantilesThreshold.
   * @param tdigest
   *          The algorihm to use for maintaining approximate quantiles. Must not be null.
   */
  public ScalableStatistics(int exactQuantilesThreshold, TDigest tdigest) {
    Preconditions.checkNotNull(tdigest);
    this.tdigest = tdigest;
    Preconditions.checkArgument(exactQuantilesThreshold >= 0);
    this.exactQuantilesThreshold = exactQuantilesThreshold;
    this.exactValues = new double[Math.min(exactQuantilesThreshold, MAX_DEFAULT_ARRAY_LENGTH)];
  }
  
  private static TDigest getDefaultTDigest() {
    //return TDigest.createDigest(COMPRESSION);
    return new AVLTreeDigest(COMPRESSION);
    //return new TreeDigest(COMPRESSION);
    //return new ArrayDigest(4, COMPRESSION);
    //return new MergingDigest(COMPRESSION);    
  }
  
  /** Adds the given data value to the data set */
  public void add(double value) {
    count++;
    min = Math.min(min, value);
    max = Math.max(max, value);
    sum += value;
    sumOfSquares += value * value;
    addQuantileValue(value);
  }
  
  private void addQuantileValue(double value) {
    if (exactValues != null && numExactValues == exactQuantilesThreshold) { 
      // dynamically switch algorithm from exact quantiles to approximate quantiles
      assert exactValues.length == numExactValues;
      for (double v : exactValues) {
        tdigest.add(v);
      }
      exactValues = null;
    }
    
    if (exactValues != null) {
      if (numExactValues == exactValues.length) { // grow array
        int newCapacity = Math.min(exactQuantilesThreshold, (numExactValues * 3) / 2 + 1);
        exactValues = Arrays.copyOf(exactValues, newCapacity);
      }
      exactValues[numExactValues++] = value;
      areExactValuesSorted = false;
    } else {
      tdigest.add(value);
    }    
  }
  
  /**
   * Merges another statistics instance into this instance. For example, this way the independent
   * stats and quantiles of each separate MapReduce task can be merged into a single overall job
   * summary statistic.
   */
  public void add(ScalableStatistics other) {
    count += other.count;
    min = Math.min(min, other.min);
    max = Math.max(max, other.max);
    sum += other.sum;
    sumOfSquares += other.sumOfSquares;
    tdigest.add(other.tdigest);
    if (other.exactValues != null) {
      for (int i = 0; i < other.numExactValues; i++) {
        addQuantileValue(other.exactValues[i]);
      }
    }
  }
  
  /** Serializes the data structure into a byte array */
  public byte[] asBytes() {
    byte[] className = tdigest.getClass().getName().getBytes(Charsets.UTF_8);
    int vlen = exactValues == null ? 0 : numExactValues;
    ByteBuffer buf = ByteBuffer.allocate(4 + 8*5 + 4 + 4 + 8*vlen + 4 + className.length + tdigest.byteSize() + 4);
    buf.putInt(MAGIC_CODE); // for sanity checks
    
    // write basic descriptive stats
    buf.putLong(count);
    buf.putDouble(min);
    buf.putDouble(max);
    buf.putDouble(sum);
    buf.putDouble(sumOfSquares);
    
    // write exact values, if any
    buf.putInt(exactQuantilesThreshold);
    buf.putInt(exactValues == null ? -1 : numExactValues);
    if (exactValues != null) {
      for (int i = 0; i < numExactValues; i++) {
        buf.putDouble(exactValues[i]);
      }
    }
    
    // write tdigest
    buf.putInt(className.length);
    buf.put(className);
    tdigest.asBytes(buf);
    
    buf.putInt(MAGIC_CODE); // for sanity checks
    Preconditions.checkArgument(buf.position() == buf.capacity());
    return buf.array();
  }
  
  /** Derializes the data structure from a byte array */
  public static ScalableStatistics fromBytes(byte[] bytes) {
    Preconditions.checkArgument(bytes.length > 0);
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    ScalableStatistics stats = new ScalableStatistics();
    Preconditions.checkArgument(buf.getInt() == MAGIC_CODE);
    
    // read basic descriptive stats
    stats.count = buf.getLong();
    stats.min = buf.getDouble();
    stats.max = buf.getDouble();
    stats.sum = buf.getDouble();
    stats.sumOfSquares = buf.getDouble();
    
    // read exact values, if any
    stats.exactQuantilesThreshold = buf.getInt();
    int vlen = buf.getInt();
    if (vlen >= 0) {
      stats.numExactValues = vlen;
      stats.exactValues = new double[vlen];
      for (int i = 0; i < vlen; i++) {
        stats.exactValues[i] = buf.getDouble();
      }
    } else {
      stats.exactValues = null;
    }
    
    // read tdigest
    byte[] classNameBytes = new byte[buf.getInt()];
    buf.get(classNameBytes);
    String className = new String(classNameBytes, Charsets.UTF_8);
    try {
      Method method = Class.forName(className).getMethod("fromBytes", ByteBuffer.class);
      Preconditions.checkArgument(Modifier.isStatic(method.getModifiers()));
      stats.tdigest = (TDigest) method.invoke(null, buf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    Preconditions.checkArgument(buf.getInt() == MAGIC_CODE);
    Preconditions.checkArgument(buf.position() == buf.capacity());
    return stats;
  }
  
  /**
   * Returns an independent copy of this instance that doesn't share backing storage with this
   * instance.
   */
  public ScalableStatistics copy() {
    return fromBytes(asBytes());
  }

  /** Returns the number of values that have been added so far */
  public long getCount() {
    return count;
  }

  /** Returns the maximum value of the data set */
  public double getMax() {
    return max;
  }

  /** Returns the minimum value of the data set */
  public double getMin() {
    return min;
  }

  /** Returns the sum of the values in the data set */
  public double getSum() {
    return sum;
  }

  /** Returns the sum of the squared values in the data set */
  public double getSumOfSquares() {
    return sumOfSquares;
  }
  
  /** Returns the mean of the data set */
  public double getMean() {
    return getSum() / getCount();
  }
  
  /** Returns the standard deviation of the data set */
  public double getStandardDeviation() {
    double variance = (getSumOfSquares() - getMean() * getSum()) / getCount();
    return Math.sqrt(variance);
  }
  
  /**
   * Returns the quantile of the data set.
   * 
   * @param quantile
   *          must satisfy 0.0 <= quantile <= 1.0. For example to read the 99% percentile pass
   *          quantile = 0.99
   */  
  public double getQuantile(double quantile) {    
    if (exactValues != null) {
      return getExactQuantile(quantile);
    } else {
      return tdigest.quantile(quantile);
    }
  }

  private double getExactQuantile(double quantile) {
    if (quantile < 0.0 || quantile > 1.0) {
      throw new IllegalArgumentException("There must hold 0.0 <= quantile <= 1.0 for: " + quantile);
    }
    if (numExactValues == 0) {
      return Double.NaN;
    }
    if (!areExactValuesSorted) {
      Arrays.sort(exactValues, 0, numExactValues);
      areExactValuesSorted = true;
    }
    double i = (numExactValues - 1) * quantile;
    int j = (int)i;
    if (j == numExactValues - 1) {
      return exactValues[j];
    } else { // interpolate
      double diff = i - j ;
      double low = exactValues[j];
      double high = exactValues[j + 1];
      return low + diff * (high - low);
    }
  }
  
  @Override
  public String toString() {
    return "count:" + getCount() + ", min:" + getMin() + ", max:" + getMax() + ", mean:" + getMean() + ", sum:"
        + getSum() + ", stddev:" + getStandardDeviation() + ", median:" + getQuantile(0.5) + ", p75:"
        + getQuantile(0.75) + ", p95:" + getQuantile(0.95) + ", p98:" + getQuantile(0.98) + ", p99:" + getQuantile(0.99)
        + ", p999:" + getQuantile(0.999);
  }
}
