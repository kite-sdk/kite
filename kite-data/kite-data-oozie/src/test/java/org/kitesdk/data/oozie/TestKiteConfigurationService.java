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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.spi.DefaultConfiguration;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

@RunWith(Parameterized.class)
public class TestKiteConfigurationService  extends MiniDFSTest {

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
  Path kiteConfigPath = null;

  public TestKiteConfigurationService(boolean distributed) {
    this.distributed = distributed;
  }

  private static Configuration originalKiteConf;

  @BeforeClass
  public static void defaultConfiguration() {
    originalKiteConf = DefaultConfiguration.get();
  }

  @AfterClass
  public static void restoreConfiguration() {
    DefaultConfiguration.set(originalKiteConf);
  }

  @After
  public void removeDataPath() throws IOException {
    // restore configuration
    DefaultConfiguration.set(startingConf);

    if(serviceTempDir != null) {
      FileUtils.deleteDirectory(serviceTempDir);
      serviceTempDir = null;
    }
    
    if(kiteConfigPath != null) {
      fs.delete(kiteConfigPath, true);
      kiteConfigPath = null;
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

    startingOozieHome = System.getProperty("oozie.home.dir");
    serviceTempDir = Files.createTempDir();
  }

  @Test
  public void noKiteConfiguration() throws ServiceException, FileNotFoundException, IOException {
    Assume.assumeTrue(!distributed);

    kiteConfigPath = new Path("target/kite-conf-serv");
    setupKiteConfigurationService(kiteConfigPath, true, false, false);
    Assert.assertNull(Services.get().get(KiteConfigurationService.class).getKiteConf());
  }

  @Test
  public void singleKiteConfiguration() throws ServiceException, IOException {
    Assume.assumeTrue(!distributed);

    kiteConfigPath = new Path("target/kite-conf-serv");
    setupKiteConfigurationService(kiteConfigPath, true, true, false);
    Configuration serviceKiteConf = Services.get().get(KiteConfigurationService.class).getKiteConf();
    Assert.assertNotNull(serviceKiteConf);
    Assert.assertEquals("test.value",serviceKiteConf.get("test.property"));
  }

  @Test
  public void multipleKiteConfigurations() throws ServiceException, IOException {
    Assume.assumeTrue(!distributed);

    kiteConfigPath = new Path("target/kite-conf-serv");
    setupKiteConfigurationService(kiteConfigPath, true, true, true);
    Configuration serviceKiteConf = Services.get().get(KiteConfigurationService.class).getKiteConf();
    Assert.assertNotNull(serviceKiteConf);
    Assert.assertEquals("test.value",serviceKiteConf.get("test.property"));
    Assert.assertEquals("second.value",serviceKiteConf.get("second.property"));
  }

  @Test
  public void hdfsKiteConfigurations() throws ServiceException, IOException {
    Assume.assumeTrue(distributed);

    kiteConfigPath = new Path("hdfs:///testing/kite-conf-serv");
    setupKiteConfigurationService(kiteConfigPath, true, true, true);
    Configuration serviceKiteConf = Services.get().get(KiteConfigurationService.class).getKiteConf();
    Assert.assertNotNull(serviceKiteConf);
    Assert.assertEquals("test.value",serviceKiteConf.get("test.property"));
    Assert.assertEquals("second.value",serviceKiteConf.get("second.property"));
  }

  private void setupKiteConfigurationService(Path kiteConfigLocation, boolean loadKiteService, 
      boolean loadPrimaryConfig, boolean loadSecondaryConfig) throws IOException, ServiceException  {
    File confDir = new File(serviceTempDir, "conf");
    File hadoopConfDir = new File(confDir, "hadoop-conf");
    File actionConfDir = new File(confDir, "action-conf");
    confDir.mkdirs();
    hadoopConfDir.mkdirs();
    actionConfDir.mkdirs();

    Path kiteConfDir = new Path(kiteConfigLocation, "kite-conf");
    fs.mkdirs(kiteConfDir);

    // these may need to be forced to local FS
    File oozieSiteTarget = new File(confDir, "oozie-site.xml");
    File hadoopConfTarget = new File(hadoopConfDir, "hadoop-site.xml");

    // we'll want tests for this with both local and hdfs files
    Path primaryConfTarget = new Path(kiteConfDir, "primary-site.xml");
    Path secondaryConfTarget = new Path(kiteConfDir, "secondary-site.xml");

    Configuration oozieSiteConf = new Configuration(false);

    List<String> configs = new ArrayList<String>();
    if(loadPrimaryConfig) {
      configs.add(primaryConfTarget.toString());
    }

    if(loadSecondaryConfig) {
      configs.add(secondaryConfTarget.toString());
    }

    if(!configs.isEmpty()) {
      oozieSiteConf.set("oozie.service.KiteConfigurationService.kite.configuration", Joiner.on(",").join(configs));
    }

    oozieSiteConf.set("oozie.services", "org.apache.oozie.service.HadoopAccessorService");
    FileOutputStream oozieStream = new FileOutputStream(oozieSiteTarget);
    oozieSiteConf.writeXml(oozieStream);
    oozieStream.close();
    FileOutputStream hadoopStream = new FileOutputStream(hadoopConfTarget);
    conf.writeXml(hadoopStream);
    hadoopStream.close();

    if(loadPrimaryConfig) {
      Configuration primaryConf = new Configuration(false);
      primaryConf.set("test.property", "test.value");

      FSDataOutputStream primaryStream = fs.create(primaryConfTarget);
      primaryConf.writeXml(primaryStream);
      primaryStream.close();
    }

    if(loadSecondaryConfig) {
      Configuration secondaryConf = new Configuration(false);
      secondaryConf.set("second.property", "second.value");

      FSDataOutputStream secondaryStream = fs.create(secondaryConfTarget);
      secondaryConf.writeXml(secondaryStream);
      secondaryStream.close();
    }

    // set to the temp directory
    System.setProperty("oozie.home.dir", serviceTempDir.getAbsolutePath());

    Services services = new Services();
    services.init();
    if(loadKiteService) {
      services.setService(KiteConfigurationService.class);
    }
  }
}
