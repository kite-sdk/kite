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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

public class KiteConfigurationService implements Service {

  public static final String CONF_PREFIX = Service.CONF_PREFIX + "KiteConfigurationService.";
  public static final String KITE_CONFIGURATION = CONF_PREFIX + "kite.configuration";

  private static XLog LOG = XLog.getLog(KiteConfigurationService.class);
  private Configuration conf;
  private Configuration kiteConf;

  @Override
  public void init(Services services) throws ServiceException {
    conf = services.getConf();
    try {
      loadKiteConf(services);
    } catch(IOException ioe) {
        throw new ServiceException(ErrorCode.E0100, KiteConfigurationService.class.getName(), "An exception occured while attempting"
                + "to load the Kite Configuration", ioe);
    }
  }

  public Configuration getKiteConf() {
    return kiteConf;
  }

  // largely borrowed with from the HCatAccessorService
  // configuration methodology. Modification includes the
  // ability to specify multiple paths to configurations
  private void loadKiteConf(Services services) throws IOException {
    String[] paths = conf.getStrings(KITE_CONFIGURATION);
    if (paths != null && paths.length != 0) {
      kiteConf = new Configuration();
      for(String path : paths) {
        if (path.startsWith("hdfs")) {
            Path p = new Path(path);
            HadoopAccessorService has = services.get(HadoopAccessorService.class);
            
            try {
                FileSystem fs = has.createFileSystem(
                        System.getProperty("user.name"), p.toUri(), has.createJobConf(p.toUri().getAuthority()));
                if (fs.exists(p)) {
                    FSDataInputStream is = null;
                    try {
                        is = fs.open(p);
                        Configuration partialConf = new XConfiguration(is);
                        kiteConf = merge(kiteConf, partialConf);
                    } finally {
                        if (is != null) {
                            is.close();
                        }
                    }
                    LOG.info("Loaded Kite Configuration: " + path);
                } else {
                    LOG.warn("Kite Configuration could not be found at [" + path + "]");
                }
            } catch (HadoopAccessorException hae) {
                throw new IOException(hae);
            }
        } else {
            File f = new File(path);
            if (f.exists()) {
                InputStream is = null;
                try {
                    is = new FileInputStream(f);
                    Configuration partialConf = new XConfiguration(is);
                    kiteConf = merge(kiteConf, partialConf);
                } finally {
                    if (is != null) {
                        is.close();
                    }
                }
                LOG.info("Loaded Kite Configuration: " + path);
            } else {
                LOG.warn("Kite Configuration could not be found at [" + path + "]");
            }
        }
      }
    } else {
      LOG.info("Kite Configuration not specified");
    }
  }

  @Override
  public void destroy() {
    //no-op
  }

  @Override
  public Class<? extends Service> getInterface() {
    return KiteConfigurationService.class;
  }

  //borrowed from spring-hadoop ConfigurationUtils. Alternative for CDH5 compatibility
  //where Configuration.addResource(configuration) is not available
  private static Configuration merge(Configuration one, Configuration two) {
    if (one == null) {
      if (two == null) {
        return new Configuration();
      }
      return new Configuration(two);
    }

    Configuration c = new Configuration(one);

    if (two == null) {
      return c;
    }

    for (Map.Entry<String, String> entry : two) {
      c.set(entry.getKey(), entry.getValue());
    }

    return c;
  }

}
