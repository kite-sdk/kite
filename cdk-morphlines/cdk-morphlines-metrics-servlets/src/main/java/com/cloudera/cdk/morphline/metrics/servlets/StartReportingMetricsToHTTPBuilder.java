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
package com.cloudera.cdk.morphline.metrics.servlets;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Notifications;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.health.jvm.ThreadDeadlockHealthCheck;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
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
      boolean showJvmMetrics = getConfigs().getBoolean(config, "showJvmMetrics", true);
      validateArguments();

      synchronized (SERVERS) {
        Server server = SERVERS.get(port);
        if (server == null) {
          if (showJvmMetrics) {
            MetricRegistry registry = context.getMetricRegistry();
            BufferPoolMetricSet bufferPoolMetrics = new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer());
            registerAll("jvm.buffers", bufferPoolMetrics, registry);
            registerAll("jvm.gc", new GarbageCollectorMetricSet(), registry);
            registerAll("jvm.memory", new MemoryUsageGaugeSet(), registry);
            registerAll("jvm.threads", new ThreadStatesGaugeSet(), registry);
            //registerAll("jvm.fileDescriptorUsageRatio", new FileDescriptorRatioGauge(), registry);
          }
          context.getHealthCheckRegistry().register("deadlocks", new ThreadDeadlockHealthCheck());

          ServletContextHandler servletContextHandler = new ServletContextHandler();
          servletContextHandler.addServlet(AdminServlet.class, "/*");
          
          servletContextHandler.addEventListener(new MetricsServlet.ContextListener() {      
            @Override
            protected MetricRegistry getMetricRegistry() {
              return context.getMetricRegistry();
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
    
    // Same method as registry.registerAll(prefix, metrics) except that it avoids an exception 
    // on registering the same metric more than once
    private void registerAll(String prefix, MetricSet metrics, MetricRegistry registry) {
      for (Map.Entry<String, Metric> entry : metrics.getMetrics().entrySet()) {
        String name = MetricRegistry.name(prefix, entry.getKey());
        if (entry.getValue() instanceof MetricSet) {
          registerAll(name, (MetricSet) entry.getValue(), registry);
        } else {
          if (!registry.getMetrics().containsKey(name)) { // this check is the diff
            registry.register(name, entry.getValue());
          }
        }
      } 
    }
    
  }
  
}
