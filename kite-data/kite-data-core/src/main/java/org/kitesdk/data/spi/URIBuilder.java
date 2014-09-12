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

package org.kitesdk.data.spi;

import java.net.URI;

/**
 * @deprecated will be removed in 0.18.0; use org.kitesdk.data.URIBuilder
 */
@Deprecated
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification="Moved to parent class, only for compatibility")
public class URIBuilder extends org.kitesdk.data.URIBuilder {
  public URIBuilder(URI uri) {
    super(uri);
  }

  public URIBuilder(String uri) {
    super(uri);
  }

  public URIBuilder(String repoUri, String dataset) {
    super(repoUri, org.kitesdk.data.URIBuilder.NAMESPACE_DEFAULT, dataset);
  }

  public URIBuilder(URI repoUri, String dataset) {
    super(repoUri, org.kitesdk.data.URIBuilder.NAMESPACE_DEFAULT, dataset);
  }

  public URIBuilder(String repoUri, String namespace, String dataset) {
    super(repoUri, namespace, dataset);
  }

  public URIBuilder(URI repoUri, String namespace, String dataset) {
    super(repoUri, namespace, dataset);
  }
}
