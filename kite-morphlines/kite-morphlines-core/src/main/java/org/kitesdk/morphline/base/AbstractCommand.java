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
package org.kitesdk.morphline.base;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

/**
 * Base class for convenient implementation of {@link Command} classes.
 */
public abstract class AbstractCommand implements Command {
  
  private final Config config;
  private final Command parent;
  private final Command child;
  private final MorphlineContext context;
  private final String name;
  private final Configs configs;
  private final Meter numProcessCallsMeter;
  private final Meter numNotifyCallsMeter;
  
  private static final boolean IS_MEASURING_METRICS = 
      "true".equals(System.getProperty("isMeasuringMetrics", "true"));

  protected final Logger LOG = LoggerFactory.getLogger(getClass());
    
  /**
   * Using the given <code>builder</code>, constructs a command rooted at the given morphline JSON
   * <code>config</code>.
   * 
   * The command will feed records into <code>child</code>. The command will have
   * <code>parent</code> as it's parent. Additional parameters can be passed via the morphline
   * <code>context</code>.
   */
  protected AbstractCommand(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
    Preconditions.checkNotNull(builder);
    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(parent);
    Preconditions.checkNotNull(child);
    Preconditions.checkNotNull(context);
    this.config = config;
    this.parent = parent;
    this.child = child;
    this.context = context;
    Preconditions.checkArgument(builder.getNames().size() > 0);
    this.name = "morphline." + builder.getNames().iterator().next();
    this.configs = new Configs();
    this.numProcessCallsMeter = getMeter(Metrics.NUM_PROCESS_CALLS);
    this.numNotifyCallsMeter = getMeter(Metrics.NUM_NOTIFY_CALLS);
  }
    
  /** Deprecated; will be removed in the next release */
  @Deprecated
  protected AbstractCommand(Config config, Command parent, Command child, MorphlineContext context) {
    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(parent);
    Preconditions.checkNotNull(child);
    Preconditions.checkNotNull(context);
    this.config = config;
    this.parent = parent;
    this.child = child;
    this.context = context;
    this.name = "morphline." + getShortClassName(getClass());
    this.configs = new Configs();
    this.numProcessCallsMeter = getMeter(Metrics.NUM_PROCESS_CALLS);
    this.numNotifyCallsMeter = getMeter(Metrics.NUM_NOTIFY_CALLS);    
  }
  
  @Override
  public Command getParent() {
    return parent;
  }
  
  /**
   * Returns the child of this command. The parent of a command A is the command B that passes
   * records to A. A is the child of B.
   */
  protected Command getChild() {
    return child;
  }
  
  /** Returns the JSON configuration of this command. */ 
  protected Config getConfig() {
    return config;
  }
  
  /** Returns the context of this command. */ 
  protected MorphlineContext getContext() {
    return context;
  }
  
  /** Returns a helper for convenient access to the JSON configuration of this command. */ 
  protected Configs getConfigs() {
    return configs;
  }
  
  @Override
  public final void notify(Record notification) {
    beforeNotify(notification);
    doNotify(notification);
  }

  private void beforeNotify(Record notification) {
    if (isMeasuringMetrics()) {
      numNotifyCallsMeter.mark();
    }
    if (LOG.isTraceEnabled()) {    
      LOG.trace("beforeNotify: {}", notification);
    } else {
      LOG.debug("beforeNotify()");
    }
  }

  /**
   * Processes the given notification on the control plane of the subtree rooted at this command.
   */
  protected void doNotify(Record notification) {
    getChild().notify(notification);
  }
  
  @Override
  public final boolean process(Record record) {
    beforeProcess(record);
    boolean success = doProcess(record);
    if (!success) {
      LOG.debug("Command failed!");
    }
    return success;
  }
  
  private void beforeProcess(Record record) {
    if (isMeasuringMetrics()) {
      numProcessCallsMeter.mark();
    }
    if (LOG.isTraceEnabled()) {    
      LOG.trace("beforeProcess: {}", record);      
    } else {
      LOG.debug("beforeProcess()");      
    }
  }
  
  /**
   * Processes the given record on the data plane of this command.
   * 
   * @return true to indicate that processing shall continue, false to indicate that backtracking
   *         shall be done
   */
  protected boolean doProcess(Record record) {
    return getChild().process(record);
  }
  
  /** Helper that checks if the user provided configuration parameters are valid. */ 
  protected void validateArguments() {
    getConfigs().validateArguments(getConfig());
  }
  
  /** Returns a counter for maintaining main-memory metrics. */
  protected Counter getCounter(String... names) {
    return getContext().getMetricRegistry().counter(getMetricName(names));
  }
  
  /** Returns a histogram for maintaining main-memory metrics. */
  protected Histogram getHistogram(String... names) {
    return getContext().getMetricRegistry().histogram(getMetricName(names));
  }
  
  /** Returns a meter for maintaining main-memory metrics. */
  protected Meter getMeter(String... names) {
    return getContext().getMetricRegistry().meter(getMetricName(names));
  }
  
  /** Returns a timer for maintaining main-memory metrics. */
  protected Timer getTimer(String... names) {
    return getContext().getMetricRegistry().timer(getMetricName(names));
  }
  
  private String getMetricName(String... names) {
    return MetricRegistry.name(name, names);
  }
  
  /** Returns whether or not metrics should be measured. */
  protected final boolean isMeasuringMetrics() {
    return IS_MEASURING_METRICS;
  }
  
  private String getShortClassName(Class clazz) {
    String className = clazz.getName();
    int i = className.lastIndexOf('.'); // regular class
    int j = className.lastIndexOf('$'); // inner class
    return className.substring(1 + Math.max(i, j));
  }

  /**
   * Factory method to create the chain of commands rooted at the given rootConfig. The last command
   * in the chain will feed records into finalChild.
   * 
   * @param ignoreNotifications
   *          if true indicates don't forward notifications at the end of the chain of commands.
   *          This is a feature that multi-branch commands like tryRules and ifThenElse need to
   *          avoid sending a notification multiple times to finalChild, once from each branch.
   */
  protected List<Command> buildCommandChain(Config rootConfig, String configKey, Command finalChild, boolean ignoreNotifications) {
    Preconditions.checkNotNull(rootConfig);
    Preconditions.checkNotNull(configKey);
    Preconditions.checkNotNull(finalChild);
    List<? extends Config> commandConfigs = new Configs().getConfigList(rootConfig, configKey, Collections.<Config>emptyList());
    List<Command> commands = Lists.newArrayList();
    Command currentParent = this;
    Connector lastConnector = null;        
    for (int i = 0; i < commandConfigs.size(); i++) {
      boolean isLast = (i == commandConfigs.size() - 1);
      Connector connector = new Connector(ignoreNotifications && isLast);
      if (isLast) {
        connector.setChild(finalChild);
      }
      Config cmdConfig = commandConfigs.get(i);
      Command cmd = buildCommand(cmdConfig, currentParent, connector);
      commands.add(cmd);
      if (i > 0) {
        lastConnector.setChild(cmd);
      }
      connector.setParent(cmd);
      currentParent = connector;
      lastConnector = connector;
    }
    return commands;
  }

  /**
   * Factory method to create a command rooted at the given cmdConfig. The command will feed records
   * into finalChild. The command will have currentParent as it's parent.
   */
  protected Command buildCommand(Config cmdConfig, Command currentParent, Command finalChild) {   
    Preconditions.checkNotNull(cmdConfig);
    Preconditions.checkNotNull(currentParent);
    Preconditions.checkNotNull(finalChild);    
    Set<Map.Entry<String, Object>> entries = cmdConfig.root().unwrapped().entrySet();
    if (entries.size() != 1) {
      throw new MorphlineCompilationException("Illegal number of entries: " + entries.size(), cmdConfig);
    }
    Map.Entry<String, Object> entry = entries.iterator().next();
    String cmdName = entry.getKey();
    
    Class cmdClass;
    LOG.trace("Building command: {}", cmdName);
    if (!cmdName.contains(".") && !cmdName.contains("/")) {
      cmdClass = getContext().getCommandBuilder(cmdName);
      if (cmdClass == null) {
        throw new MorphlineCompilationException("No command builder registered for name: " + cmdName, cmdConfig);
      }
    } else {
      String className = cmdName.replace('/', '.');
      try {
        cmdClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new MorphlineCompilationException("Cannot find command class: " + className, cmdConfig, e);
      }
    }
    Object obj;
    try {
      obj = cmdClass.newInstance();
    } catch (Exception e) {
      throw new MorphlineCompilationException("Cannot instantiate command class: " + cmdClass.getName(), cmdConfig, e);
    }
    if (!(obj instanceof CommandBuilder)) {
      throw new MorphlineCompilationException("Type of command " + cmdName + " must be an instance of "
          + CommandBuilder.class.getName() + " but is: " + cmdClass.getName(), cmdConfig);
    } 
    CommandBuilder builder = (CommandBuilder) obj;
    Command cmd = builder.build(cmdConfig.getConfig(cmdName), currentParent, finalChild, getContext());
    return cmd;
  }
  
}
