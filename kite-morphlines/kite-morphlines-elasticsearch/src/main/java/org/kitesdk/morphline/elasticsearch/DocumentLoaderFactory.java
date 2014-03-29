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

package org.kitesdk.morphline.elasticsearch;

import com.typesafe.config.Config;
import java.util.List;
import org.kitesdk.morphline.api.MorphlineRuntimeException;

/**
 *
 */
public class DocumentLoaderFactory {
  public static final String TRANSPORT_DOCUMENT_LOADER = "transport";
  public static final String REST_DOCUMENT_LOADER = "rest";

  public static final String HOSTS_FIELD = "hosts";
  public static final String CLUSTER_NAME_FIELD = "clusterName";

  /**
   *
   * @param clientType String representation of client type
   * @param config Configuration of DocumentLoader
   *
   * @return
   */
  public DocumentLoader getClient(String clientType, Config config) throws IllegalArgumentException {
    List<String> hostNames = config.getStringList(HOSTS_FIELD);

    if (hostNames == null) {
      throw new MorphlineRuntimeException("No parameter " + HOSTS_FIELD + " set");
    }

    if (clientType.equalsIgnoreCase(TRANSPORT_DOCUMENT_LOADER)) {
      String clusterName = config.getString(CLUSTER_NAME_FIELD);
      if (clusterName == null) {
        throw new MorphlineRuntimeException("No parameter " + CLUSTER_NAME_FIELD + " set");
      }
      return new TransportDocumentLoader(hostNames, clusterName);
    } else if (clientType.equalsIgnoreCase(REST_DOCUMENT_LOADER)) {
      return new RestDocumentLoader(hostNames);
    }
    throw new IllegalArgumentException("There is no such a client. " + clientType);
  }

  /**
   * Used for tests only. Creates local elasticsearch instance client.
   *
   * @param clientType Name of client to use
   *
   * @return Local elastic search instance client
   */
  public DocumentLoader getLocalClient(String clientType) throws IllegalArgumentException {
    if (clientType.equalsIgnoreCase(TRANSPORT_DOCUMENT_LOADER)) {
      return new TransportDocumentLoader();
    } else if (clientType.equalsIgnoreCase(REST_DOCUMENT_LOADER)) {
      return new RestDocumentLoader();
    }
    throw new IllegalArgumentException("There is no such a client. " + clientType);
  }
}
