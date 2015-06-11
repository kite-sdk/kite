/**
 * Copyright 2015 Cloudera Inc.
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
package org.kitesdk.data.oozie;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.hadoop.LauncherException;
import org.apache.oozie.action.hadoop.LauncherURIHandler;

public class KiteLauncherURIHandler implements LauncherURIHandler {

  @Override
  public boolean create(final URI uri, final Configuration conf)
      throws LauncherException {
    throw new UnsupportedOperationException(
        "Creation of resources is not supported for Kite URIs.");
  }

  @Override
  public boolean delete(final URI uri, final Configuration conf)
      throws LauncherException {
    throw new UnsupportedOperationException(
        "Deletion of resources is not supported for Kite URIs.");
  }

  @Override
  public List<Class<?>> getClassesForLauncher() {
    return Collections.emptyList();
  }

}
