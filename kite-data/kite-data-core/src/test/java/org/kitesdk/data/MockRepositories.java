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

package org.kitesdk.data;

import com.google.common.collect.Maps;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.URIPattern;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockRepositories {
  private static final AtomicInteger ids = new AtomicInteger(0);
  private static final Map<String, DatasetRepository> repos = Maps.newHashMap();
  private static boolean registered = false;

  private static void registerMockRepoBuilder() {
    final URIPattern mockPattern = new URIPattern("mock::id");
    Registration.register(
        mockPattern, mockPattern,
        new OptionBuilder<DatasetRepository>() {
          @Override
          public DatasetRepository getFromOptions(Map<String, String> options) {
            DatasetRepository repo = repos.get(options.get("id"));
            if (repo == null) {
              repo = mock(org.kitesdk.data.spi.DatasetRepository.class);
              when(repo.getUri()).thenReturn(
                  URI.create("repo:" + mockPattern.construct(options)));
              repos.put(options.get("id"), repo);
            }
            return repo;
          }
        }
    );
  }

  private static void ensureRegistered() {
    if (!registered) {
      registerMockRepoBuilder();
    }
  }

  public static DatasetRepository newMockRepository() {
    ensureRegistered();
    String uri = "repo:mock:" + Integer.toString(ids.incrementAndGet());
    return DatasetRepositories.repositoryFor(uri);
  }
}
