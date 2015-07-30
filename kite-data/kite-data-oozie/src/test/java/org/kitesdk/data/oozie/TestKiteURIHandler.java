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

import org.kitesdk.data.PartitionView;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Formats;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.View;
import org.kitesdk.data.oozie.KiteURIHandler;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.URIPattern;
import org.kitesdk.data.spi.filesystem.FileSystemDatasetRepository;
import com.google.common.io.Files;

@RunWith(Parameterized.class)
public class TestKiteURIHandler extends MiniDFSTest {

  protected static final String NAMESPACE = "ns1";
  protected static final String NAME = "provider_test1";

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
        { false },  // default to local FS
        { true } }; // default to distributed FS
    return Arrays.asList(data);
  }

  // whether this should use the DFS provided by MiniDFSTest
  protected boolean distributed;

  protected Configuration conf;
  protected DatasetDescriptor testDescriptor;
  protected FileSystem fs;
  private Configuration startingConf;
  private String startingOozieHome;

  private File serviceTempDir;

  public TestKiteURIHandler(boolean distributed) {
    this.distributed = distributed;
  }

  public DatasetRepository newRepo() {
    return new FileSystemDatasetRepository.Builder()
        .configuration(conf)
        .rootDirectory(URI.create("target/data"))
        .build();
  }

  @After
  public void removeDataPath() throws IOException {
    fs.delete(new Path("target/data"), true);
    // restore configuration
    DefaultConfiguration.set(startingConf);

    if(serviceTempDir != null) {
      FileUtils.deleteDirectory(serviceTempDir);
      serviceTempDir = null;
    }
    if(Services.get() != null) {
      Services.get().destroy();
    }

    if(startingOozieHome == null) {
      System.clearProperty("oozie.home.dir");
    } else {
      System.setProperty("oozie.home.dir", startingOozieHome);
      startingOozieHome = null;
    }
  }

  @Before
  public void setUp() throws IOException, URISyntaxException {
    this.conf = (distributed ?
        MiniDFSTest.getConfiguration() :
        new Configuration());

    this.fs = FileSystem.get(conf);

    this.testDescriptor = new DatasetDescriptor.Builder()
        .format(Formats.AVRO)
        .schema(SchemaBuilder.record("Event").fields()
            .requiredLong("timestamp")
            .requiredString("message")
            .endRecord())
        .partitionStrategy(new PartitionStrategy.Builder()
            .year("timestamp")
            .month("timestamp")
            .day("timestamp")
            .build())
        .build();

    uriHandler = new KiteURIHandler();

    startingConf = DefaultConfiguration.get();

    startingOozieHome = System.getProperty("oozie.home.dir");
  }

  private KiteURIHandler uriHandler;

  @Test
  public void uriToNonExistantDatasetsView() throws URIHandlerException, URISyntaxException {
    URI uri = new URI("view:file:target/data/data/nomailbox?message=hello");
    Assert.assertFalse("URIs to datasets that don't exist should return false",
        uriHandler.exists(uri, null));
  }

  @Test
  public void supportedSchemes() {
    uriHandler.init(new Configuration());
    Set<String> scheme = new HashSet<String>();
    scheme.add("view");
    scheme.add("dataset");
    Assert.assertEquals(scheme, uriHandler.getSupportedSchemes());
  }

  @Test (expected = UnsupportedOperationException.class)
  public void registerNotifications() throws URISyntaxException, URIHandlerException {
    URI uri = new URI("view:hdfs://localhost:9083/default/person?version=201404240000");

    uriHandler.registerForNotification(uri,new Configuration(), "user","234");
  }

  @Test (expected = UnsupportedOperationException.class)
  public void unregisterNotifications() throws URISyntaxException {
    URI uri = new URI("view:hdfs://localhost:9083/default/person?version=201404240000");

    uriHandler.unregisterFromNotification(uri, "2324");
  }

  @Test
  public void checkURIDoesNotExist() throws URIHandlerException, IOException{
    DatasetRepository repository = newRepo();
    Dataset<GenericRecord> dataset = repository.create("data","notreadymailbox", testDescriptor);

    RefinableView<GenericRecord> view = dataset.with("message", "hello");

    Assert.assertFalse(uriHandler.exists(view.getUri(), null));
  }

  @Test
  public void checkURIExistsView() throws URIHandlerException, IOException{
    DatasetRepository repository = newRepo();
    Dataset<GenericRecord> dataset = repository.create("data","readymailbox", testDescriptor);

    View<GenericRecord> view = dataset.with("message", "hello");
    ((Signalable<GenericRecord>)view).signalReady();

    Assert.assertTrue(uriHandler.exists(view.getUri(), null));
  }

  @Test
  public void checkURIExistsDataset() throws URIHandlerException, IOException{
    DatasetRepository repository = newRepo();
    Dataset<GenericRecord> dataset = repository.create("data","readymailbox", testDescriptor);

    ((Signalable<GenericRecord>)((View<GenericRecord>)dataset)).signalReady();

    Assert.assertTrue(uriHandler.exists(dataset.getUri(), null));
  }

  @Test
  public void validateInvalidScheme() throws URIHandlerException {
    String uri = "repo:hdfs:/default/cloudera/users?favoriteColor=pink";
    try {
      uriHandler.validate(uri);
      Assert.fail("Validate with an invalid schema should have thrown an exception");
    } catch (XException ex) {
      Assert.assertEquals(ErrorCode.E0904, ex.getErrorCode());
    }
  }

  @Test
  public void validateNotAURI() throws URIHandlerException {
    String uri = "clearly not a uri";
    try {
      uriHandler.validate(uri);
      Assert.fail("Validate with an invalid URI should have thrown an exception");
    } catch (XException ex) {
      Assert.assertEquals(ErrorCode.E0906, ex.getErrorCode());
    }
  }

  @Test
  public void existsForNonReadiableView() throws URIHandlerException, URISyntaxException {
    Registration.register(
        new URIPattern("unreadiable?absolute=true"),
        new URIPattern("unreadiable::namespace/:dataset?absolute=true"),
        new UnreadiableDatasetBuilder());

    URI uri = new URI("view:unreadiable:default/person?version=201404240000");

    Assert.assertFalse(uriHandler.exists(uri, null));
  }

  @Test
  public void loadConfigFromHCatAccessor() throws URIHandlerException, URISyntaxException, ServiceException, IOException {
    setupKiteConfigurationService(true, true);
    URI uri = new URI("view:file:target/data/data/nomailbox?message=hello");
    uriHandler.exists(uri, null);

    Configuration defaultConf = DefaultConfiguration.get();
    Assert.assertEquals("test.value", defaultConf.get("test.property"));

    Services.get().get(KiteConfigurationService.class).getKiteConf().set("test.value", "something.else");
    // doesn't modify default config on further exist calls
    uriHandler.exists(uri, null);

    defaultConf = DefaultConfiguration.get();
    Assert.assertEquals("test.value", defaultConf.get("test.property"));
    Assert.assertEquals("something.else", Services.get().get(KiteConfigurationService.class).getKiteConf().get("test.value"));
  }

  @Test
  public void noConfigLoadedWhenNoServices() throws URIHandlerException, URISyntaxException {
    URI uri = new URI("view:file:target/data/data/nomailbox?message=hello");
    uriHandler.exists(uri, null);
    Assert.assertNull(Services.get());
  }

  @Test
  public void noConfigLoadedWhenNoKiteService() throws URIHandlerException, URISyntaxException, ServiceException, FileNotFoundException, IOException {
    setupKiteConfigurationService(false, false);

    URI uri = new URI("view:file:target/data/data/nomailbox?message=hello");
    uriHandler.exists(uri, null);
    Assert.assertNotNull(Services.get());
    Assert.assertNull(Services.get().get(KiteConfigurationService.class));
  }

  @Test
  public void noConfigLoadedIfNoKiteServiceConfig() throws URIHandlerException, URISyntaxException, ServiceException, FileNotFoundException, IOException {
    setupKiteConfigurationService(true, false);
    URI uri = new URI("view:file:target/data/data/nomailbox?message=hello");
    uriHandler.exists(uri, null);
    Assert.assertNotNull(Services.get());
    Assert.assertNotNull(Services.get().get(KiteConfigurationService.class));
    Assert.assertNull(Services.get().get(KiteConfigurationService.class).getKiteConf());
  }

  private void setupKiteConfigurationService(boolean loadKiteService, boolean loadKiteConfig) throws ServiceException, FileNotFoundException, IOException {
    serviceTempDir = Files.createTempDir();
    File confDir = new File(serviceTempDir, "conf");
    File hadoopConfDir = new File(confDir, "hadoop-conf");
    File hadoopConfTarget = new File(hadoopConfDir, "hadoop-site.xml");
    File actionConfDir = new File(confDir, "action-conf");
    File hiveConfDir = new File(confDir, "hive-conf");
    File hiveConfTarget = new File(hiveConfDir, "hive-site.xml");
    File oozieSiteTarget = new File(confDir, "oozie-site.xml");
    confDir.mkdir();
    hadoopConfDir.mkdir();
    actionConfDir.mkdir();
    hiveConfDir.mkdir();

    Configuration oozieSiteConf = new Configuration(false);

    if(loadKiteConfig) {
      oozieSiteConf.set("oozie.service.KiteConfigurationService.kite.configuration", hiveConfTarget.getAbsolutePath());
    }

    oozieSiteConf.set("oozie.services", "org.apache.oozie.service.HadoopAccessorService");
    oozieSiteConf.writeXml(new FileOutputStream(oozieSiteTarget));
    conf.writeXml(new FileOutputStream(hadoopConfTarget));

    Configuration hiveConf = new Configuration(false);
    hiveConf.set("test.property", "test.value");
    hiveConf.writeXml(new FileOutputStream(hiveConfTarget));

    // set to the temp directory
    System.setProperty("oozie.home.dir", serviceTempDir.getAbsolutePath());

    Services services = new Services();
    services.init();
    if(loadKiteService) {
      services.setService(KiteConfigurationService.class);
    }
  }

  //minimal implementation of the dataset stack to get an non-readiable view to load in the handler
  private static final class UnreadiableDatasetRepository implements DatasetRepository {

    @Override
    public <E> Dataset<E> load(String namespace, String name) {
      return null;
    }

    @Override
    public <E> Dataset<E> load(String namespace, String name, Class<E> type) {
      return new UnreadiableDataset<E>();
    }

    @Override
    public <E> Dataset<E> create(String namespace, String name, DatasetDescriptor descriptor) {
      return null;
    }

    @Override
    public <E> Dataset<E> create(String namespace, String name, DatasetDescriptor descriptor, Class<E> type) {
      return null;
    }

    @Override
    public <E> Dataset<E> update(String namespace, String name, DatasetDescriptor descriptor) {
      return null;
    }

    @Override
    public <E> Dataset<E> update(String namespace, String name, DatasetDescriptor descriptor, Class<E> type) {
      return null;
    }

    @Override
    public boolean delete(String namespace, String name) {
      return false;
    }

    @Override
    public boolean exists(String namespace, String name) {
      return false;
    }

    @Override
    public Collection<String> namespaces() {
      return null;
    }

    @Override
    public Collection<String> datasets(String namespace) {
      return null;
    }

    @Override
    public URI getUri() {
      return null;
    }

  }

  @SuppressWarnings("rawtypes")
  private static final class UnreadiableDataset<E> implements Dataset<E> {

    @Override
    public RefinableView<E> with(String name, Object... values) {
      return null;
    }

    @Override
    public RefinableView<E> from(String name, Comparable value) {
      return null;
    }

    @Override
    public RefinableView<E> fromAfter(String name, Comparable value) {
      return null;
    }

    @Override
    public RefinableView<E> to(String name, Comparable value) {
      return null;
    }

    @Override
    public RefinableView<E> toBefore(String name, Comparable value) {
      return null;
    }

    @Override
    public Dataset<E> getDataset() {
      return null;
    }

    @Override
    public DatasetReader<E> newReader() {
      return null;
    }

    @Override
    public DatasetWriter<E> newWriter() {
      return null;
    }

    @Override
    public boolean includes(E entity) {
      return false;
    }

    @Override
    public boolean deleteAll() {
      return false;
    }

    @Override
    public Iterable<PartitionView<E>> getCoveringPartitions() {
      return null;
    }

    @Override
    public Class<E> getType() {
      return null;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public String getNamespace() {
      return null;
    }

    @Override
    public DatasetDescriptor getDescriptor() {
      return null;
    }

    @Override
    public URI getUri() {
      return null;
    }

    @Override
    public Schema getSchema() {
        return null;
    }

    @Override
    public View<GenericRecord> asSchema(Schema schema) {
        return null;
    }

    @Override
    public <T> View<T> asType(Class<T> type) {
        return null;
    }

  }

  private static final class UnreadiableDatasetBuilder implements OptionBuilder<DatasetRepository> {

    @Override
    public DatasetRepository getFromOptions(Map<String, String> options) {
      return new UnreadiableDatasetRepository();
    }

  }

}
