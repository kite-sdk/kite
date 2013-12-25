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
package org.kitesdk.morphline.metrics.servlets;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Notifications;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.AdminServlet;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.typesafe.config.Config;

/**
 * Command that exposes liveness status, health check status, metrics state and thread dumps via a
 * set of HTTP URLs served by Jetty, using the AdminServlet.
 */
public final class StartReportingMetricsToHTTPBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("startReportingMetricsToHTTP");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new StartReportingMetricsToHTTP(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class StartReportingMetricsToHTTP extends AbstractCommand {

    private final int port;
    
    private static final Map<Integer, Server> SERVERS = new HashMap();
    
    public StartReportingMetricsToHTTP(CommandBuilder builder, Config config, Command parent, 
                                       Command child, final MorphlineContext context) {
      
      super(builder, config, parent, child, context);      

      this.port = getConfigs().getInt(config, "port", 8080);
      final TimeUnit defaultDurationUnit = getConfigs().getTimeUnit(config, "defaultDurationUnit", TimeUnit.MILLISECONDS);
      final TimeUnit defaultRateUnit = getConfigs().getTimeUnit(config, "defaultRateUnit", TimeUnit.SECONDS); 
      validateArguments();

      synchronized (SERVERS) {
        Server server = SERVERS.get(port);
        if (server == null) {
          ServletContextHandler servletContextHandler = new ServletContextHandler();
          servletContextHandler.addServlet(AdminServlet.class, "/*");
          
          servletContextHandler.addEventListener(new MetricsServlet.ContextListener() {      
            @Override
            protected MetricRegistry getMetricRegistry() {
              return context.getMetricRegistry();
            }
            @Override
            protected TimeUnit getRateUnit() {
              return defaultRateUnit;
            }
            @Override
            protected TimeUnit getDurationUnit() {
              return defaultDurationUnit;
            }
          });
          
          servletContextHandler.addEventListener(new HealthCheckServlet.ContextListener() {         
            @Override
            protected HealthCheckRegistry getHealthCheckRegistry() {
              return context.getHealthCheckRegistry();
            }
          });
          
          server = new Server(port);
          server.setHandler(servletContextHandler);
          
          try {
            server.start();
          } catch (Exception e) {
            throw new MorphlineRuntimeException(e);
          }
          
          SERVERS.put(port, server);
        }
      }
    }
        
    @Override
    protected void doNotify(Record notification) {
      for (Object event : Notifications.getLifecycleEvents(notification)) {
        if (event == Notifications.LifecycleEvent.SHUTDOWN) {
          synchronized (SERVERS) {
            Server server = SERVERS.remove(port);
            if (server != null) {
              try {
                server.stop();
                server.join();
              } catch (Exception e) {
                throw new MorphlineRuntimeException(e);
              }
            }
          }
        }
      }
      super.doNotify(notification);
    }
    
  }
  
}
