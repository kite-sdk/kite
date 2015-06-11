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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.action.hadoop.LauncherURIHandler;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.XLog;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.View;

/**
 * A Kite URI handler that works with {@link Signalable} views.
 *
 * To be considered as {@link #exists(URI, Configuration, String) existing} the
 * view must have been signaled as ready.
 */
public class KiteURIHandler implements URIHandler {

  private static final XLog LOG = XLog.getLog(KiteURIHandler.class);

  private Set<String> supportedSchemes;
  private List<Class<?>> classesToShip;

  @Override
  public void init(final Configuration conf) {
    supportedSchemes = new HashSet<String>();
    final String[] schemes = conf.getStrings(
        URIHandlerService.URI_HANDLER_SUPPORTED_SCHEMES_PREFIX
            + this.getClass().getSimpleName()
            + URIHandlerService.URI_HANDLER_SUPPORTED_SCHEMES_SUFFIX, "view", "dataset");
    supportedSchemes.addAll(Arrays.asList(schemes));
    classesToShip = new KiteLauncherURIHandler().getClassesForLauncher();
  }

  @Override
  public Set<String> getSupportedSchemes() {
    return supportedSchemes;
  }

  @Override
  public Class<? extends LauncherURIHandler> getLauncherURIHandlerClass() {
    return KiteLauncherURIHandler.class;
  }

  @Override
  public List<Class<?>> getClassesForLauncher() {
    return classesToShip;
  }

  @Override
  public DependencyType getDependencyType(final URI uri)
      throws URIHandlerException {
    return DependencyType.PULL;
  }

  @Override
  public void registerForNotification(final URI uri, final Configuration conf,
      final String user, final String actionID) throws URIHandlerException {
    throw new UnsupportedOperationException(
        "Notifications are not supported for " + uri);
  }

  @Override
  public boolean unregisterFromNotification(final URI uri, final String actionID) {
    throw new UnsupportedOperationException(
        "Notifications are not supported for " + uri);
  }

  @Override
  public Context getContext(final URI uri, final Configuration conf,
      final String user) throws URIHandlerException {
    return null;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean exists(final URI uri, final Context context) throws URIHandlerException {
    try {
      View<GenericRecord> view = Datasets.load(uri);
      if(view instanceof Signalable) {
        return ((Signalable)view).isReady();
      }
    } catch (IllegalArgumentException ex) {
      LOG.error("the URI " + uri + " was not a view or dataset");
    } catch (DatasetNotFoundException ex) {
      LOG.error("the dataset for the URI "+ uri +" did not actually exist");
    } catch (DatasetException e) {
      throw new HadoopAccessorException(ErrorCode.E0902, e);
    }
    // didn't meet all the requirements for a URI/view that we want to use with oozie
    return false;
  }

  @Override
  public boolean exists(final URI uri, final Configuration conf, final String user)
      throws URIHandlerException {
    // currently does not handle access limitations between the oozie user and
    // a potential dataset owner
    return exists(uri, null);
  }

  @Override
  public String getURIWithDoneFlag(final String uri, final String doneFlag)
      throws URIHandlerException {
    return uri;
  }

  @Override
  public void validate(final String uri) throws URIHandlerException {
    try {
      URI uriParsed = URI.create(uri);
      String scheme = uriParsed.getScheme();
      if(!(URIBuilder.VIEW_SCHEME.equals(scheme) || URIBuilder.DATASET_SCHEME.equals(scheme))) {
        LOG.error("Unexpected scheme: view, uri was "+ uri);
        XException xException = new XException(ErrorCode.E0904, scheme, uri);
        throw new URIHandlerException(xException);
      }
    } catch (IllegalArgumentException iae) {
      XException xException = new XException(ErrorCode.E0906, iae);
      throw new URIHandlerException(xException);
    }
  }

  @Override
  public void destroy() {
  }

}
