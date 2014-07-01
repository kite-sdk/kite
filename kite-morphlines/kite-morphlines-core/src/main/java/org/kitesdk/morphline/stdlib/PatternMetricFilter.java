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
package org.kitesdk.morphline.stdlib;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.shaded.org.apache.hadoop.fs.GlobPattern;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;


/**
 * A PatternMetricFilter uses pattern matching with include/exclude specifications to determine if a
 * given metric shall be reported to the output destination.
 * 
 * A metric consists of a metric name and a metric class name. A metric matches the filter if the
 * metric matches at least one include specification, but matches none of the exclude
 * specifications. An include/exclude specification consists of zero or more expression pairs. Each
 * expression pair consists of an expression for the metric name, as well as an expression for the
 * metric's class name. Each expression can be a regex pattern (e.g. "regex:foo.*") or glob pattern
 * (e.g. "glob:foo*") or literal pattern (e.g. "literal:foo") or "*" which is equivalent to
 * "glob:*". Each expression pair defines one expression for the metric name and another expression
 * for the metric class name.
 * 
 * If the include specification is absent it defaults to MATCH ALL. If the exclude specification is
 * absent it defaults to MATCH NONE.
 */
final class PatternMetricFilter implements MetricFilter {
  
  private final List<ExpressionPair> includes = new ArrayList<ExpressionPair>();
  private final List<ExpressionPair> excludes = new ArrayList<ExpressionPair>();

  public static MetricFilter parse(Configs configs, Config config) {
    Preconditions.checkNotNull(configs);
    Preconditions.checkNotNull(config);
    Config filterConfig = configs.getConfig(config, "metricFilter", null);
    if (filterConfig == null) {
      return MetricFilter.ALL;
    } else {
      return new PatternMetricFilter(filterConfig);
    }
  }
  
  private PatternMetricFilter(Config config) {
    Configs configs = new Configs();
    Config includesConfig = configs.getConfig(config, "includes", null);
    if (includesConfig == null) {
      includes.add(new ExpressionPair(new MatchAllExpression(), new MatchAllExpression()));
    } else {
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(includesConfig)) {
        includes.add(parseExpressionPair(entry.getKey(), entry.getValue().toString(), includesConfig));
      }
    }
    Config excludesConfig = configs.getConfig(config, "excludes", null);
    if (excludesConfig != null) {
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(excludesConfig)) {
        excludes.add(parseExpressionPair(entry.getKey(), entry.getValue().toString(), excludesConfig));
      }
    }
    configs.validateArguments(config);
  }
  
  private ExpressionPair parseExpressionPair(String strA, String strB, Config config) {
    Expression exprA = parseExpression(strA, config);
    Expression exprB = parseExpression(strB, config);
    return new ExpressionPair(exprA, exprB);
  }

  private Expression parseExpression(String expr, Config config) {
    if (expr.equals("*")) {
      expr = "glob:*";
    }
    int i = expr.indexOf(':');
    if (i < 0) {
      throw new MorphlineCompilationException("Illegal match expression: " + expr, config);
    }
    String type = expr.substring(0, i);
    String pattern = expr.substring(i + 1, expr.length());
    if (type.equals("literal")) {
      return new LiteralExpression(pattern);
    } else if (type.equals("regex")) {
      if (pattern.equals(".*")) {
        return new MatchAllExpression(); // optimization
      }
      return new RegexExpression(Pattern.compile(pattern));
    } else if (type.equals("glob")) {
      if (pattern.equals("*")) {
        return new MatchAllExpression(); // optimization
      }
      return new GlobExpression(pattern);
    } else {
      throw new MorphlineCompilationException("Illegal match type: " + type, config);
    }
  }

  @Override
  public boolean matches(String name, Metric metric) {
    Preconditions.checkNotNull(name);
    String className = metric.getClass().getName();
    boolean isIncluded = false;
    for (ExpressionPair include : includes) {
      isIncluded = include.matches(name, className);
      if (isIncluded) {
        break;
      }
    }
    if (!isIncluded) {
      return false;
    }
    for (ExpressionPair exclude : excludes) {
      if (exclude.matches(name, className)) {
        return false;
      }
    }
    return true;
  }
  
  @Override
  public String toString() {
    return "includes: " + includes + ", excludes: " + excludes;
  }


  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public final class ExpressionPair {
    
    private final Expression exprA;
    private final Expression exprB;
    
    public ExpressionPair(Expression exprA, Expression exprB) {
      this.exprA = exprA;
      this.exprB = exprB;
    }
    
    public boolean matches(String strA, String strB) {
      return exprA.matches(strA) && exprB.matches(strB);
    }
    
    @Override
    public String toString() {
      return exprA.toString() + " : " + exprB.toString();
    }

  }
  

  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public interface Expression {
    boolean matches(String str);
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public final class MatchAllExpression implements Expression {
    
    @Override
    public boolean matches(String str) {
      return true;
    }
    
    @Override
    public String toString() {
      return "all:";
    }

  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public final class LiteralExpression implements Expression {
    
    private final String pattern;
    
    public LiteralExpression(String pattern) {
      this.pattern = pattern;
    }
    
    @Override
    public boolean matches(String str) {
      return pattern.equals(str);
    }

    @Override
    public String toString() {
      return "literal:" + pattern;
    }

  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public final class RegexExpression implements Expression {
    
    private final Pattern regex;
    
    public RegexExpression(Pattern pattern) {
      this.regex = pattern;
    }
    
    @Override
    public boolean matches(String str) {
      return regex.matcher(str).matches();
    }

    @Override
    public String toString() {
      return "regex:" + regex.pattern();
    }

  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public class GlobExpression implements Expression {
    
    private final Pattern regex;
    private final String pattern;
    
    public GlobExpression(String pattern) {
      this.pattern = pattern;
      this.regex = GlobPattern.compile(pattern);
    }
    
    @Override
    public boolean matches(String str) {
      return regex.matcher(str).matches();
    }

    @Override
    public String toString() {
      return "glob:" + pattern;
    }

  }

}
