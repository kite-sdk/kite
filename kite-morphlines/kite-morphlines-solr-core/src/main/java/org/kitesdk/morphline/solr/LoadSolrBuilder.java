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
package org.kitesdk.morphline.solr;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.retry.MetricsFacade;
import org.apache.solr.client.solrj.retry.RetryPolicyFactory;
import org.apache.solr.common.SolrInputDocument;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.api.TypedSettings;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Notifications;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * A command that loads (or deletes) a record into a SolrServer or MapReduce SolrOutputFormat.
 */
public final class LoadSolrBuilder implements CommandBuilder {

  public static final String SOLR_LOCATOR_PARAM = "solrLocator";
  public static final String LOAD_SOLR_DELETE_BY_ID = "_loadSolr_deleteById";
  public static final String LOAD_SOLR_DELETE_BY_QUERY = "_loadSolr_deleteByQuery";
  public static final String LOAD_SOLR_CHILD_DOCUMENTS = "_loadSolr_childDocuments";
  
  private static final boolean DISABLE_RETRY_POLICY_BY_DEFAULT = Boolean.parseBoolean(System.getProperty(
      LoadSolrBuilder.class.getName() + ".disableRetryPolicyByDefault", "false"));

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("loadSolr");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new LoadSolr(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class LoadSolr extends AbstractCommand {
    
    private final DocumentLoader loader;
    private final Map<String, Float> boosts = new HashMap();
    private final RateLimiter rateLimiter;
    private final Timer elapsedTime;    
    private final boolean isDryRun;
    
    public LoadSolr(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      Config solrLocatorConfig = getConfigs().getConfig(config, SOLR_LOCATOR_PARAM);
      SolrLocator locator = new SolrLocator(solrLocatorConfig, context);
      LOG.debug("solrLocator: {}", locator);
      RetryPolicyFactory retryPolicyFactory = parseRetryPolicyFactory(
          getConfigs().getConfig(config, "retryPolicy", null));
      this.loader = locator.getLoader(retryPolicyFactory, new CodahaleMetricsFacade(context.getMetricRegistry()));

      Config boostsConfig = getConfigs().getConfig(config, "boosts", ConfigFactory.empty());
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(boostsConfig)) {
        String fieldName = entry.getKey();        
        float boost = Float.parseFloat(entry.getValue().toString().trim());
        boosts.put(fieldName, boost);
      }
      this.rateLimiter = RateLimiter.create(getConfigs().getDouble(config, "maxRecordsPerSecond", Double.MAX_VALUE));
      this.isDryRun = context.getTypedSettings().getBoolean(TypedSettings.DRY_RUN_SETTING_NAME, false);
      validateArguments();
      this.elapsedTime = getTimer(Metrics.ELAPSED_TIME);
    }

    private RetryPolicyFactory parseRetryPolicyFactory(Config retryPolicyConfig) {      
      if (retryPolicyConfig == null && !DISABLE_RETRY_POLICY_BY_DEFAULT) {
        // ask RetryPolicyFactoryParser to return a retry policy with reasonable defaults
        retryPolicyConfig = ConfigFactory.parseString(
            "{" + RetryPolicyFactoryParser.BOUNDED_EXPONENTIAL_BACKOFF_RETRY_NAME + "{}}");
      }
      if (retryPolicyConfig == null) {
        return null;
      } else {
        return new RetryPolicyFactoryParser().parse(retryPolicyConfig);
      }
    }

    @Override
    protected void doNotify(Record notification) {
      for (Object event : Notifications.getLifecycleEvents(notification)) {
        if (event == Notifications.LifecycleEvent.BEGIN_TRANSACTION) {
          try {
            loader.beginTransaction();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        } else if (event == Notifications.LifecycleEvent.COMMIT_TRANSACTION) {
          try {
            loader.commitTransaction();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        }
        else if (event == Notifications.LifecycleEvent.ROLLBACK_TRANSACTION) {
          try {
            loader.rollbackTransaction();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        }
        else if (event == Notifications.LifecycleEvent.SHUTDOWN) {
          try {
            loader.shutdown();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        }
      }
      super.doNotify(notification);
    }
    
    @Override
    protected boolean doProcess(Record record) {
      rateLimiter.acquire(); 
      Timer.Context timerContext = elapsedTime.time();
      List deleteById = record.get(LOAD_SOLR_DELETE_BY_ID);
      List deleteByQuery = record.get(LOAD_SOLR_DELETE_BY_QUERY);
      try {
        if (deleteById.size() == 0 && deleteByQuery.size() == 0) {
          SolrInputDocument doc = convert(record);
          if (isDryRun) {
            System.out.println("dryrun: update: " + doc);        
          } else {
            loader.load(doc);
          }
        } else {
          for (Object id : deleteById) {
            if (isDryRun) {
              System.out.println("dryrun: deleteById: " + id.toString());
            } else {
              loader.deleteById(id.toString());
            }
          }
          for (Object query : deleteByQuery) {
            if (isDryRun) {
              System.out.println("dryrun: deleteByQuery: " + query.toString());
            } else {
              loader.deleteByQuery(query.toString());
            }
          }
        }
      } catch (IOException e) {
        throw new MorphlineRuntimeException(e);
      } catch (SolrServerException e) {
        throw new MorphlineRuntimeException(e);
      } finally {
        timerContext.stop();
      }
      
      // pass record to next command in chain:      
      return super.doProcess(record);
    }
    
    private SolrInputDocument convert(Record record) {
      Map<String, Collection<Object>> map = record.getFields().asMap();
      SolrInputDocument doc = new SolrInputDocument(new HashMap(2 * map.size()));
      for (Map.Entry<String, Collection<Object>> entry : map.entrySet()) {
        String key = entry.getKey();
        if (LOAD_SOLR_CHILD_DOCUMENTS.equals(key)) {
          for (Object value : entry.getValue()) {
            if (value instanceof Record) {
              value = convert((Record) value); // recurse
            }
            if (value instanceof SolrInputDocument) {
              doc.addChildDocument((SolrInputDocument) value);
            } else {
              throw new MorphlineRuntimeException("Child document must be of class " + 
                Record.class.getName() + " or " + SolrInputDocument.class.getName() + ": " + value);
            }
          }
        } else {
          Collection<Object> values = entry.getValue();
          if (values.size() == 1 && values.iterator().next() instanceof Map) {
            doc.setField(key, values.iterator().next(), getBoost(key)); // it is an atomic update
          } else {
            doc.setField(key, values, getBoost(key));
          }
        }
      }      
      return doc;
    }

    private float getBoost(String key) {
      if (boosts.size() > 0) {
        Float boost = boosts.get(key);
        if (boost != null) {
          return boost.floatValue();
        }
      }
      return 1.0f;
    }
    
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * A facade using codahale metrics as a backend.
   */
  private static final class CodahaleMetricsFacade implements MetricsFacade {
    
    private final MetricRegistry registry;

    public CodahaleMetricsFacade(MetricRegistry registry) {
      Preconditions.checkNotNull(registry);
      this.registry = registry;
    }
    
    @Override
    public void markMeter(String name, long increment) {
      registry.meter(name).mark(increment);
    }

    @Override
    public void updateHistogram(String name, long value) {
      registry.histogram(name).update(value);
    }
    
    @Override
    public void updateTimer(String name, long duration, TimeUnit unit) {
      registry.timer(name).update(duration, unit);
    }
    
  }

}
