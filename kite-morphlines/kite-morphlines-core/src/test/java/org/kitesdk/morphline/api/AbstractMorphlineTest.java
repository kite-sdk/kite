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
package org.kitesdk.morphline.api;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Notifications;
import org.kitesdk.morphline.stdlib.PipeBuilder;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

public class AbstractMorphlineTest extends Assert {
  
  /**
   * Contains the list of all output records that the morphline emitted, available via
   * {@link Collector#getRecords()}. This can be used to compare expected vs. actual results.
   */
  protected Collector collector; 
  
  /**
   * The morphline to run for this unit test.
   */
  protected Command morphline;
  
  /**
   * The morphline context to use for this unit test.
   */
  protected MorphlineContext morphContext;
  
  /** 
   * The directory in which morphline config files and sample input data files are located. 
   */
  protected static final String RESOURCES_DIR = "target/test-classes";
  
  @Before
  public void setUp() throws Exception {
    collector = new Collector();
  }
  
  @After
  public void tearDown() throws Exception {
    collector = null;
    morphline = null;
    morphContext = null;
  }
    
  protected Command createMorphline(String file, Config... overrides) throws IOException {
    return createMorphline(parse(file, overrides));
  }

  protected Command createMorphline(Config config) {
    morphContext = new MorphlineContext.Builder().setMetricRegistry(new MetricRegistry()).build();
    return new PipeBuilder().build(config, null, collector, morphContext);
  }
  
  protected Config parse(String file, Config... overrides) throws IOException {
    Config config = new Compiler().parse(new File(RESOURCES_DIR + "/" + file + ".conf"), overrides);
    config = config.getConfigList("morphlines").get(0);
    Preconditions.checkNotNull(config);
    return config;
  }
  
  protected void deleteAllDocuments() {
    collector.reset();
  }
  
  protected void startSession() {
    Notifications.notifyStartSession(morphline);
  }

  @SuppressWarnings("unchecked")
  protected static <T> T[] concat(T[]... arrays) {    
    if (arrays.length == 0) throw new IllegalArgumentException();
    Class clazz = null;
    int length = 0;
    for (T[] array : arrays) {
      clazz = array.getClass();
      length += array.length;
    }
    T[] result = (T[]) Array.newInstance(clazz.getComponentType(), length);
    int pos = 0;
    for (T[] array : arrays) {
      System.arraycopy(array, 0, result, pos, array.length);
      pos += array.length;
    }
    return result;
  }

}
