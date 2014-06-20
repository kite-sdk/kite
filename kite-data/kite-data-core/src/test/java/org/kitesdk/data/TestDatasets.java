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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.URIBuilder;
import org.kitesdk.data.spi.URIPattern;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestDatasets {
  private static final AtomicInteger ids = new AtomicInteger(0);
  private static final Map<String, DatasetRepository> repos = Maps.newHashMap();

  @BeforeClass
  public static void addMockRepoBuilder() throws Exception {
    final URIPattern mockPattern = new URIPattern("mock::id");
    Registration.register(
        mockPattern, mockPattern,
        new OptionBuilder<DatasetRepository>() {
          @Override
          public DatasetRepository getFromOptions(Map<String, String> options) {
            DatasetRepository repo = repos.get(options.get("id"));
            if (repo == null) {
              repo = mock(DatasetRepository.class);
              when(repo.getUri()).thenReturn(
                  URI.create("repo:" + mockPattern.construct(options)));
              repos.put(options.get("id"), repo);
            }
            return repo;
          }
        }
    );
  }

  public DatasetRepository newMockRepository() {
    String uri = "repo:mock:" + Integer.toString(ids.incrementAndGet());
    return DatasetRepositories.open(uri);
  }

  private DatasetRepository repo = null;
  private URI repoUri = null;

  @Before
  public void setupMock() {
    this.repo = newMockRepository();
    this.repoUri = repo.getUri();
    verify(repo).getUri(); // verify the above getUri() call
  }

  @Test
  public void testCreate() {
    URI datasetUri = new URIBuilder(repoUri, "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<Object> expected = mock(Dataset.class);
    when(repo.create("test", descriptor)).thenReturn(expected);

    Dataset<Object> ds = Datasets.create(datasetUri, descriptor);

    verify(repo).create("test", descriptor);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testCreateStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<Object> expected = mock(Dataset.class);
    when(repo.create("test", descriptor)).thenReturn(expected);

    Dataset<Object> ds = Datasets.create(datasetUri.toString(), descriptor);

    verify(repo).create("test", descriptor);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testCreateView() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();

    Dataset<Object> ds = mock(Dataset.class);
    when(repo.create("test", descriptor)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    RefinableView<Object> userView = mock(RefinableView.class);
    when(ds.with("username", "user1")).thenReturn(userView);

    RefinableView<Object> userAndEmailView = mock(RefinableView.class);
    when(userView.with("email", "user1@example.com"))
        .thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    RefinableView<Object> view = Datasets.create(datasetUri, descriptor);

    verify(repo).create("test", descriptor);
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).with("username", "user1");
    verifyNoMoreInteractions(ds);

    verify(userView).with("email", "user1@example.com");
    verifyNoMoreInteractions(userView);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testCreateViewStringUri() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();

    Dataset<Object> ds = mock(Dataset.class);
    when(repo.create("test", descriptor)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    RefinableView<Object> userView = mock(RefinableView.class);
    when(ds.with("username", "user1")).thenReturn(userView);

    RefinableView<Object> userAndEmailView = mock(RefinableView.class);
    when(userView.with("email", "user1@example.com"))
        .thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    RefinableView<Object> view = Datasets
        .create(datasetUri.toString(), descriptor);

    verify(repo).create("test", descriptor);
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).with("username", "user1");
    verifyNoMoreInteractions(ds);

    verify(userView).with("email", "user1@example.com");
    verifyNoMoreInteractions(userView);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testLoad() {
    Dataset<Object> expected = mock(Dataset.class);
    when(repo.load("test")).thenReturn(expected);

    URI datasetUri = new URIBuilder(repoUri, "test").build();

    Dataset<Object> ds = Datasets.load(datasetUri);

    verify(repo).load("test");
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testLoadStringUri() {
    Dataset<Object> expected = mock(Dataset.class);
    when(repo.load("test")).thenReturn(expected);

    URI datasetUri = new URIBuilder(repoUri, "test").build();

    Dataset<Object> ds = Datasets.load(datasetUri);

    verify(repo).load("test");
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testLoadView() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();

    Dataset<Object> ds = mock(Dataset.class);
    when(repo.load("test")).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    RefinableView<Object> userView = mock(RefinableView.class);
    when(ds.with("username", "user1")).thenReturn(userView);

    RefinableView<Object> userAndEmailView = mock(RefinableView.class);
    when(userView.with("email", "user1@example.com"))
        .thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    RefinableView<Object> view = Datasets.load(datasetUri);

    verify(repo).load("test");
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).with("username", "user1");
    verifyNoMoreInteractions(ds);

    verify(userView).with("email", "user1@example.com");
    verifyNoMoreInteractions(userView);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testLoadViewStringUri() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();

    Dataset<Object> ds = mock(Dataset.class);
    when(repo.load("test")).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    RefinableView<Object> userView = mock(RefinableView.class);
    when(ds.with("username", "user1")).thenReturn(userView);

    RefinableView<Object> userAndEmailView = mock(RefinableView.class);
    when(userView.with("email", "user1@example.com"))
        .thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    RefinableView<Object> view = Datasets.load(datasetUri.toString());

    verify(repo).load("test");
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).with("username", "user1");
    verifyNoMoreInteractions(ds);

    verify(userView).with("email", "user1@example.com");
    verifyNoMoreInteractions(userView);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testDelete() {
    URI datasetUri = new URIBuilder(repoUri, "test").build();

    Datasets.delete(datasetUri);

    verify(repo).delete("test");
    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testDeleteStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "test").build();

    Datasets.delete(datasetUri.toString());

    verify(repo).delete("test");
    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testDeleteRejectsViewUri() {
    final URI datasetUri = new URIBuilder(repoUri, "test")
        .with("field", 34)
        .build();

    TestHelpers.assertThrows("Should reject view URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.delete(datasetUri);
          }
        });

    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testExists() {
    URI datasetUri = new URIBuilder(repoUri, "test").build();

    when(repo.exists("test")).thenReturn(true);

    Assert.assertTrue(Datasets.exists(datasetUri));

    verify(repo).exists("test");
    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testExistsStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "test").build();

    when(repo.exists("test")).thenReturn(false);

    Assert.assertFalse(Datasets.exists(datasetUri.toString()));

    verify(repo).exists("test");
    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testExistsRejectsViewUri() {
    final URI datasetUri = new URIBuilder(repoUri, "test")
        .with("field", 34)
        .build();

    TestHelpers.assertThrows("Should reject view URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.exists(datasetUri);
          }
        });

    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testUpdate() {
    URI datasetUri = new URIBuilder(repoUri, "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<Object> expected = mock(Dataset.class);
    when(repo.update("test", descriptor)).thenReturn(expected);

    Dataset<Object> ds = Datasets.update(datasetUri, descriptor);

    verify(repo).update("test", descriptor);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testUpdateStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<Object> expected = mock(Dataset.class);
    when(repo.update("test", descriptor)).thenReturn(expected);

    Dataset<Object> ds = Datasets.update(datasetUri.toString(), descriptor);

    verify(repo).update("test", descriptor);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testUpdateRejectsViewUri() {
    final URI datasetUri = new URIBuilder(repoUri, "test")
        .with("field", 34)
        .build();
    final DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    TestHelpers.assertThrows("Should reject view URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.<Object, Dataset<Object>>update(datasetUri, descriptor);
          }
        });

    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testRepositoryFor() {
    URI datasetUri = new URIBuilder(repoUri, "test").build();

    Assert.assertEquals(repo, Datasets.repositoryFor(datasetUri));

    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testRepositoryForStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "test").build();

    Assert.assertEquals(repo, Datasets.repositoryFor(datasetUri.toString()));

    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testRepositoryForView() {
    URI datasetUri = new URIBuilder(repoUri, "test")
        .with("field", 34)
        .build();

    Assert.assertEquals(repo, Datasets.repositoryFor(datasetUri));

    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testRepositoryForViewStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "test")
        .with("field", 34)
        .build();

    Assert.assertEquals(repo, Datasets.repositoryFor(datasetUri.toString()));

    verifyNoMoreInteractions(repo);
  }

}
