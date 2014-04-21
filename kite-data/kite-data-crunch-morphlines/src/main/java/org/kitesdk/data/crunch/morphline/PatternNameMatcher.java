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
package org.kitesdk.data.crunch.morphline;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.GlobPattern;

import com.google.common.base.Preconditions;


/**
 * A Predicate that uses pattern matching with include/exclude specifications to determine if a
 * given string matches.
 * 
 * A string matches the predicate if the string matches at least one include expression, but matches
 * none of the exclude expressions. An include/exclude expression can be a regex pattern (e.g.
 * "regex:foo.*") or glob pattern (e.g. "glob:foo*") or literal pattern (e.g. "literal:foo") or "*"
 * which is equivalent to "glob:*".
 * 
 * If the include specification is absent it defaults to MATCH ALL. If the exclude specification is
 * absent it defaults to MATCH NONE.
 */
final class PatternNameMatcher implements NameMatcher {
  
  private final List<Expression> includes = new ArrayList();
  private final List<Expression> excludes = new ArrayList();

  public static NameMatcher parse(List<String> includeExpressions, List<String> excludeExpressions) {
    return new PatternNameMatcher(includeExpressions, excludeExpressions);
  }
  
  private PatternNameMatcher(List<String> includeExpressions, List<String> excludeExpressions) {
    for (String includeExpression : includeExpressions) {
      includes.add(parseExpression(includeExpression));
    }
    for (String excludeExpression : excludeExpressions) {
      excludes.add(parseExpression(excludeExpression));
    }
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
      return new RegexExpression(Pattern.compile(pattern));
    } else if (type.equals("glob")) {
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
  
  @Override
  public String toString() {
    return "includes: " + includes + ", excludes: " + excludes;
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
