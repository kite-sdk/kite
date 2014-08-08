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

import com.google.common.collect.Lists;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.URIBuilder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestDatasets {
  private DatasetRepository repo = null;
  private URI repoUri = null;

  @Before
  public void setupMock() {
    this.repo = MockRepositories.newMockRepository();
    this.repoUri = repo.getUri();
    verify(repo).getUri(); // verify the above getUri() call
  }

  @Test
  public void testCreate() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<Object> expected = mock(Dataset.class);
    when(repo.create("ns", "test", descriptor, Object.class)).thenReturn(expected);

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        create(datasetUri, descriptor, Object.class);

    verify(repo).create("ns", "test", descriptor, Object.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testCreateWithoutType() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<GenericRecord> expected = mock(Dataset.class);
    when(repo.create("ns", "test", descriptor, GenericRecord.class)).thenReturn(expected);

    Dataset<GenericRecord> ds = Datasets.create(datasetUri, descriptor);

    verify(repo).create("ns", "test", descriptor, GenericRecord.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test(expected=NullPointerException.class)
  public void testCreateNullType() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Datasets.<Object, Dataset<Object>> create(datasetUri, descriptor, null);
  }

  @Test
  public void testCreateStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<Object> expected = mock(Dataset.class);
    when(repo.create("ns", "test", descriptor, Object.class)).thenReturn(expected);

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        create(datasetUri.toString(), descriptor, Object.class);

    verify(repo).create("ns", "test", descriptor, Object.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testCreateStringUriWithoutType() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<GenericRecord> expected = mock(Dataset.class);
    when(repo.create("ns", "test", descriptor, GenericRecord.class)).thenReturn(expected);

    Dataset<GenericRecord> ds = Datasets.create(datasetUri.toString(), descriptor);

    verify(repo).create("ns", "test", descriptor, GenericRecord.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testCreateView() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
    Constraints constraints = new Constraints(descriptor.getSchema(), null)
        .with("username", "user1")
        .with("email", "user1@example.com");

    AbstractDataset<Object> ds = mock(AbstractDataset.class);
    when(repo.create("ns", "test", descriptor, Object.class)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    AbstractRefinableView<Object> userAndEmailView = mock(AbstractRefinableView.class);
    when(ds.filter(constraints)).thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    RefinableView<Object> view = Datasets.<Object, RefinableView<Object>>
        create(datasetUri, descriptor, Object.class);

    verify(repo).create("ns", "test", descriptor, Object.class);
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).filter(constraints);
    verifyNoMoreInteractions(ds);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testCreateViewWithoutType() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
    Constraints constraints = new Constraints(descriptor.getSchema(), null)
        .with("username", "user1")
        .with("email", "user1@example.com");

    AbstractDataset<GenericRecord> ds = mock(AbstractDataset.class);
    when(repo.create("ns", "test", descriptor, GenericRecord.class)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    AbstractRefinableView<GenericRecord> userAndEmailView = mock(AbstractRefinableView.class);
    when(ds.filter(constraints)).thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    View<GenericRecord> view = Datasets.create(datasetUri, descriptor);

    verify(repo).create("ns", "test", descriptor, GenericRecord.class);
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).filter(constraints);
    verifyNoMoreInteractions(ds);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testCreateViewStringUri() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
    Constraints constraints = new Constraints(descriptor.getSchema(), null)
        .with("username", "user1")
        .with("email", "user1@example.com");

    AbstractDataset<Object> ds = mock(AbstractDataset.class);
    when(repo.create("ns", "test", descriptor, Object.class)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    AbstractRefinableView<Object> userAndEmailView = mock(AbstractRefinableView.class);
    when(ds.filter(constraints)).thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    RefinableView<Object> view = Datasets.<Object, RefinableView<Object>>
        create(datasetUri.toString(), descriptor, Object.class);

    verify(repo).create("ns", "test", descriptor, Object.class);
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).filter(constraints);
    verifyNoMoreInteractions(ds);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testCreateViewStringUriWithoutType() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
    Constraints constraints = new Constraints(descriptor.getSchema(), null)
        .with("username", "user1")
        .with("email", "user1@example.com");

    AbstractDataset<GenericRecord> ds = mock(AbstractDataset.class);
    when(repo.create("ns", "test", descriptor, GenericRecord.class)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    AbstractRefinableView<GenericRecord> userAndEmailView = mock(AbstractRefinableView.class);
    when(ds.filter(constraints)).thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    View<GenericRecord> view = Datasets.create(datasetUri.toString(), descriptor);

    verify(repo).create("ns", "test", descriptor, GenericRecord.class);
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).filter(constraints);
    verifyNoMoreInteractions(ds);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testLoad() {
    Dataset<Object> expected = mock(Dataset.class);
    when(repo.load("ns", "test", Object.class)).thenReturn(expected);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        load(datasetUri, Object.class);

    verify(repo).load("ns", "test", Object.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testLoadWithoutType() {
    Dataset<GenericRecord> expected = mock(Dataset.class);
    when(repo.load("ns", "test", GenericRecord.class)).thenReturn(expected);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    Dataset<GenericRecord> ds = Datasets.load(datasetUri);

    verify(repo).load("ns", "test", GenericRecord.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test(expected=NullPointerException.class)
  public void testLoadNullType() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    Datasets.<Object, Dataset<Object>> load(datasetUri, null);
  }

  @Test
  public void testLoadStringUri() {
    Dataset<Object> expected = mock(Dataset.class);
    when(repo.load("ns", "test", Object.class)).thenReturn(expected);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        load(datasetUri, Object.class);

    verify(repo).load("ns", "test", Object.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testLoadStringUriWithoutType() {
    Dataset<GenericRecord> expected = mock(Dataset.class);
    when(repo.load("ns", "test", GenericRecord.class)).thenReturn(expected);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    Dataset<GenericRecord> ds = Datasets.load(datasetUri);

    verify(repo).load("ns", "test", GenericRecord.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testLoadView() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
    Constraints constraints = new Constraints(descriptor.getSchema(), null)
        .with("username", "user1")
        .with("email", "user1@example.com");

    AbstractDataset<Object> ds = mock(AbstractDataset.class);
    when(repo.load("ns", "test", Object.class)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    AbstractRefinableView<Object> userAndEmailView = mock(AbstractRefinableView.class);
    when(ds.filter(constraints)).thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    RefinableView<Object> view = Datasets.<Object, RefinableView<Object>>
        load(datasetUri, Object.class);

    verify(repo).load("ns", "test", Object.class);
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).filter(constraints);
    verifyNoMoreInteractions(ds);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testLoadViewWithoutType() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
    Constraints constraints = new Constraints(descriptor.getSchema(), null)
        .with("username", "user1")
        .with("email", "user1@example.com");

    AbstractDataset<GenericRecord> ds = mock(AbstractDataset.class);
    when(repo.load("ns", "test", GenericRecord.class)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    AbstractRefinableView<GenericRecord> userAndEmailView = mock(AbstractRefinableView.class);
    when(ds.filter(constraints)).thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    RefinableView<GenericRecord> view = Datasets.load(datasetUri);

    verify(repo).load("ns", "test", GenericRecord.class);
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).filter(constraints);
    verifyNoMoreInteractions(ds);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testLoadViewStringUri() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
    Constraints constraints = new Constraints(descriptor.getSchema(), null)
        .with("username", "user1")
        .with("email", "user1@example.com");

    AbstractDataset<Object> ds = mock(AbstractDataset.class);
    when(repo.load("ns", "test", Object.class)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    AbstractRefinableView<Object> userAndEmailView = mock(AbstractRefinableView.class);
    when(ds.filter(constraints)).thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    RefinableView<Object> view = Datasets.<Object, RefinableView<Object>>
        load(datasetUri.toString(), Object.class);

    verify(repo).load("ns", "test", Object.class);
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).filter(constraints);
    verifyNoMoreInteractions(ds);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testLoadViewStringUriWithoutType() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
    Constraints constraints = new Constraints(descriptor.getSchema(), null)
        .with("username", "user1")
        .with("email", "user1@example.com");

    AbstractDataset<GenericRecord> ds = mock(AbstractDataset.class);
    when(repo.load("ns", "test", GenericRecord.class)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(descriptor);

    AbstractRefinableView<GenericRecord> userAndEmailView = mock(AbstractRefinableView.class);
    when(ds.filter(constraints)).thenReturn(userAndEmailView);

    URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("username", "user1")
        .with("email", "user1@example.com")
        .with("ignoredOption", "abc")
        .build();

    RefinableView<GenericRecord> view = Datasets.load(datasetUri.toString());

    verify(repo).load("ns", "test", GenericRecord.class);
    verifyNoMoreInteractions(repo);

    verify(ds).getDescriptor();
    verify(ds).filter(constraints);
    verifyNoMoreInteractions(ds);

    verifyNoMoreInteractions(userAndEmailView);

    Assert.assertEquals(userAndEmailView, view);
  }

  @Test
  public void testDelete() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    Datasets.delete(datasetUri);

    verify(repo).delete("ns", "test");
    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testDeleteStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    Datasets.delete(datasetUri.toString());

    verify(repo).delete("ns", "test");
    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testDeleteRejectsViewUri() {
    final URI datasetUri = new URIBuilder(repoUri, "ns", "test")
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
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    when(repo.exists("ns", "test")).thenReturn(true);

    Assert.assertTrue(Datasets.exists(datasetUri));

    verify(repo).exists("ns", "test");
    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testExistsStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    when(repo.exists("ns", "test")).thenReturn(false);

    Assert.assertFalse(Datasets.exists(datasetUri.toString()));

    verify(repo).exists("ns", "test");
    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testExistsRejectsViewUri() {
    final URI datasetUri = new URIBuilder(repoUri, "ns", "test")
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
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<Object> expected = mock(Dataset.class);
    when(repo.update("ns", "test", descriptor, Object.class)).thenReturn(expected);

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        update(datasetUri, descriptor, Object.class);

    verify(repo).update("ns", "test", descriptor, Object.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testUpdateWithoutType() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<GenericRecord> expected = mock(Dataset.class);
    when(repo.update("ns", "test", descriptor, GenericRecord.class)).thenReturn(expected);

    Dataset<GenericRecord> ds = Datasets.update(datasetUri, descriptor);

    verify(repo).update("ns", "test", descriptor, GenericRecord.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test(expected=NullPointerException.class)
  public void testUpdateNullType() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Datasets.<Object, Dataset<Object>> update(datasetUri, descriptor, null);
  }

  @Test
  public void testUpdateStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<Object> expected = mock(Dataset.class);
    when(repo.update("ns", "test", descriptor, Object.class)).thenReturn(expected);

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        update(datasetUri.toString(), descriptor, Object.class);

    verify(repo).update("ns", "test", descriptor, Object.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testUpdateStringUriWithoutType() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<GenericRecord> expected = mock(Dataset.class);
    when(repo.update("ns", "test", descriptor, GenericRecord.class)).thenReturn(expected);

    Dataset<GenericRecord> ds = Datasets.update(datasetUri.toString(), descriptor);

    verify(repo).update("ns", "test", descriptor, GenericRecord.class);
    verifyNoMoreInteractions(repo);

    verifyNoMoreInteractions(expected);

    Assert.assertEquals(expected, ds);
  }

  @Test
  public void testUpdateRejectsViewUri() {
    final URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("field", 34)
        .build();
    final DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    TestHelpers.assertThrows("Should reject view URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.<Object, Dataset<Object>>update(datasetUri, descriptor, Object.class);
          }
        });

    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testListRejectsDatasetUri() {
    TestHelpers.assertThrows("Should reject dataset URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.list(new URIBuilder(repoUri, "ns", "test").build());
          }
        });
    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testListRejectsViewUri() {
    TestHelpers.assertThrows("Should reject dataset URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.list(new URIBuilder(repoUri, "ns", "test")
                .with("field", 34)
                .build());
          }
        });
    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testListRepoUri() {
    when(repo.namespaces()).thenReturn(Lists.newArrayList("ns"));
    when(repo.datasets("ns")).thenReturn(Lists.newArrayList("a", "b", "c"));
    List<URI> expected = Lists.newArrayList(
        new URIBuilder(repoUri, "ns", "a").build(),
        new URIBuilder(repoUri, "ns", "b").build(),
        new URIBuilder(repoUri, "ns", "c").build()
    );

    Collection<URI> datasetUris = Datasets.list(repoUri);

    verify(repo, times(2)).getUri(); // called in @Before and Datasets.list
    verify(repo).namespaces();
    verify(repo).datasets("ns");
    verifyNoMoreInteractions(repo);

    Assert.assertEquals(expected, datasetUris);
  }

  @Test
  public void testListStringRepoUri() {
    when(repo.namespaces()).thenReturn(Lists.newArrayList("ns"));
    when(repo.datasets("ns")).thenReturn(Lists.newArrayList("a", "b", "c"));
    List<URI> expected = Lists.newArrayList(
        new URIBuilder(repoUri, "ns", "a").build(),
        new URIBuilder(repoUri, "ns", "b").build(),
        new URIBuilder(repoUri, "ns", "c").build()
    );

    Collection<URI> datasetUris = Datasets.list(repoUri.toString());

    verify(repo, times(2)).getUri(); // called in @Before and Datasets.list
    verify(repo).namespaces();
    verify(repo).datasets("ns");
    verifyNoMoreInteractions(repo);

    Assert.assertEquals(expected, datasetUris);
  }

  @Test
  public void testRepositoryFor() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    Assert.assertEquals(repo, DatasetRepositories.repositoryFor(datasetUri));

    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testRepositoryForStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test").build();

    Assert.assertEquals(repo, DatasetRepositories.repositoryFor(datasetUri.toString()));

    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testRepositoryForView() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("field", 34)
        .build();

    Assert.assertEquals(repo, DatasetRepositories.repositoryFor(datasetUri));

    verifyNoMoreInteractions(repo);
  }

  @Test
  public void testRepositoryForViewStringUri() {
    URI datasetUri = new URIBuilder(repoUri, "ns", "test")
        .with("field", 34)
        .build();

    Assert.assertEquals(repo, DatasetRepositories.repositoryFor(datasetUri.toString()));

    verifyNoMoreInteractions(repo);
  }

}
