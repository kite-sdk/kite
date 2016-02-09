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
package org.apache.solr.client.solrj.retry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrServerException;

/**
 * Exception thrown by {@link RetryingSolrServer} when an attempt to perform a solrj request (e.g.
 * update, delete, query, commit) fails after a bunch of retries.
 */
public final class RetriesExhaustedException extends SolrServerException {

  RetriesExhaustedException(String msg, Map<String, MutableLong> counters, Throwable cause) {
    super(msg + getRootCauseCountersAsString(counters), cause);        
  }
  
  private static String getRootCauseCountersAsString(Map<String, MutableLong> counters) {
    // print sorted descending by numOccurances for human readability
    List<Map.Entry<String, MutableLong>> entries = 
        new ArrayList<Map.Entry<String, MutableLong>>(counters.entrySet());
    Collections.sort(entries, new Comparator<Map.Entry<String, MutableLong>>() {
      @Override
      public int compare(Entry<String, MutableLong> o1, Entry<String, MutableLong> o2) {
        return (int) (o2.getValue().get() - o1.getValue().get());
      }
    });
    
    StringBuilder rootCauses = new StringBuilder();
    for (Map.Entry<String, MutableLong> entry : entries) {
      if (rootCauses.length() > 0) {
        rootCauses.append(", ");
      }
      rootCauses.append(entry.getKey());
      rootCauses.append(": Occurred ");
      rootCauses.append(entry.getValue());
      rootCauses.append(" times");
    }
    return rootCauses.toString();
  }
  
}
