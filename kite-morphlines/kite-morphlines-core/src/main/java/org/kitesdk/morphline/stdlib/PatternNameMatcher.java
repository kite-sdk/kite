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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kitesdk.morphline.shaded.org.apache.hadoop.fs.GlobPattern;

import com.google.common.base.Preconditions;


/**
 * A Predicate that uses pattern matching with include/exclude specifications to determine if a
 * given string matches.
 * 
 * A string matches the predicate if the string matches at least one include expression, but matches
 * none of the exclude expressions. An include/exclude expression can be a regex pattern (e.g.
 * "regex:foo.*") or glob pattern (e.g. "glob:foo*") or literal pattern (e.g. "literal:foo") or "*"
 * which is equivalent to "glob:*".
 */
final class PatternNameMatcher implements NameMatcher {
  
  private final Expression[] includes;
  private final Expression[] excludes;

  public static NameMatcher parse(List<String> includeExpressions, List<String> excludeExpressions) {
    return new PatternNameMatcher(includeExpressions, excludeExpressions);
  }
  
  private PatternNameMatcher(List<String> includeExpressions, List<String> excludeExpressions) {
    includes = parseExpressions(includeExpressions);
    excludes = parseExpressions(excludeExpressions);
  }
  
  private Expression[] parseExpressions(List<String> expressions) {
    Expression[] parsedExpressions = new Expression[expressions.size()];
    for (int i = 0; i < expressions.size(); i++) {
      parsedExpressions[i] = parseExpression(expressions.get(i));
    }
    return parsedExpressions;
  }
  
  private Expression parseExpression(String expr) {
    if (expr.equals("*")) {
      expr = "glob:*";
    }
    int i = expr.indexOf(':');
    if (i < 0) {
      throw new IllegalArgumentException("Illegal match expression: " + expr);
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
      throw new IllegalArgumentException("Illegal match type: " + type);
    }
  }

  @Override
  public boolean matches(String name) {
    Preconditions.checkNotNull(name);
    boolean isIncluded = false;
    for (Expression include : includes) {
      isIncluded = include.matches(name);
      if (isIncluded) {
        break;
      }
    }
    if (!isIncluded) {
      return false;
    }
    for (Expression exclude : excludes) {
      if (exclude.matches(name)) {
        return false;
      }
    }
    return true;
  }
  

  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static interface Expression {
    boolean matches(String str);
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static final class MatchAllExpression implements Expression {
    
    @Override
    public boolean matches(String str) {
      return true;
    }
    
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static final class LiteralExpression implements Expression {
    
    private final String pattern;
    
    public LiteralExpression(String pattern) {
      this.pattern = pattern;
    }
    
    @Override
    public boolean matches(String str) {
      return pattern.equals(str);
    }

  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static final class RegexExpression implements Expression {
    
    private final Matcher regex;
    
    public RegexExpression(Pattern pattern) {
      this.regex = pattern.matcher("");
    }
    
    @Override
    public boolean matches(String str) {
      return regex.reset(str).matches();
    }

  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static final class GlobExpression implements Expression {
    
    private final Matcher regex;
    
    public GlobExpression(String pattern) {
      this.regex = GlobPattern.compile(pattern).matcher("");
    }
    
    @Override
    public boolean matches(String str) {
      return regex.reset(str).matches();
    }

  }

}
