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
package org.kitesdk.data.crunch.morphline;


import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;


/**
 * Abstract base class for custom logic that transforms a List of Crunch PCollections into another
 * List of Crunch PCollections. For example, implementations of this API can join multiple data
 * sources.
 */
public abstract class PipelineFn {
  
  //public void configure(Pipeline pipeline, List<Pair<String, String>> params);

  public abstract List<PCollection> process(List<PCollection> collections, Pipeline pipeline, List<Pair<String, String>> params);

  /** Convenient helper that converts a list of parameters to a Map */
  protected static <K,T> Map<K, T> toMap(List<Pair<K, T>> params) { 
    Map<K, T> parameterMap = new LinkedHashMap<K, T>();
    for (Pair <K, T> pair : params) {
      parameterMap.put(pair.first(), pair.second());
    }
    return parameterMap;
  }

}
