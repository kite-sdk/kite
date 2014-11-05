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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
final class PatternNameMatcher {
  
  private final Expression[] includes;
  private final Expression[] excludes;
  private Set<String> includeLiterals = new HashSet<String>(16, 0.5f);
  private Set<String> excludeLiterals = new HashSet<String>(16, 0.5f);
  private final String[] literalsOnly;

  public PatternNameMatcher(List<String> includeExpressions, List<String> excludeExpressions, int cacheCapacity) {
    includes = parseExpressions(includeExpressions, includeLiterals, cacheCapacity);
    excludes = parseExpressions(excludeExpressions, excludeLiterals, cacheCapacity);
    includeLiterals.removeAll(excludeLiterals);
    includeLiterals = optimize(includeLiterals);
    excludeLiterals = optimize(excludeLiterals);
    if (includes.length == 0 && excludes.length == 0) {
      literalsOnly = includeLiterals.toArray(new String[includeLiterals.size()]);
    } else {
      literalsOnly = null;
    }
  }
  
  /** Expert mode; For optional performance optimizations */
  public String[] getLiteralsOnly() {
    return literalsOnly;
  }
  
  private Set<String> optimize(Set<String> items) {
    if (items.size() == 1) {
      return Collections.singleton(items.iterator().next());
    } else {
      return items;
    }    
  }
  
  private Expression[] parseExpressions(List<String> expressions, Set<String> literals, int cacheCapacity) {
    List<Expression> parsedExpressions = new ArrayList<Expression>();
    for (int i = 0; i < expressions.size(); i++) {
      Expression expr = parseExpression(expressions.get(i), literals, cacheCapacity);
      if (expr != null) {
        parsedExpressions.add(expr);
      }
    }
    return parsedExpressions.toArray(new Expression[parsedExpressions.size()]);
  }
  
  private Expression parseExpression(String expr, Set<String> literals, int cacheCapacity) {
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
      //return new LiteralExpression(pattern);
      literals.add(pattern);
      return null;
    } else if (type.equals("regex")) {
      if (pattern.equals(".*")) {
        return new MatchAllExpression(); // optimization
      }
      return new RegexExpression(Pattern.compile(pattern), cacheCapacity);
    } else if (type.equals("glob")) {
      if (pattern.equals("*")) {
        return new MatchAllExpression(); // optimization
      }
      return new RegexExpression(GlobPattern.compile(pattern), cacheCapacity);
    } else {
      throw new IllegalArgumentException("Illegal match type: " + type);
    }
  }

  public boolean matches(String name) {
    Preconditions.checkNotNull(name);
    if (excludeLiterals.size() > 0 && excludeLiterals.contains(name)) {
      return false;
    }
    boolean isIncluded = includeLiterals.size() > 0 && includeLiterals.contains(name);
    for (int i = 0; !isIncluded && i < includes.length; i++) {
      isIncluded = includes[i].matches(name);
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
  private static interface Expression {
    boolean matches(String str);
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class MatchAllExpression implements Expression {
    
    @Override
    public boolean matches(String str) {
      return true;
    }
    
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class RegexExpression implements Expression {
    
    private final Matcher regex;
    private final Map<String, Boolean> cache;
    
    public RegexExpression(Pattern pattern, int cacheCapacity) {
      this.regex = pattern.matcher("");
      if (cacheCapacity < 0) {
        throw new IllegalArgumentException("Cache capacity must not be negative");
      }
      this.cache = cacheCapacity > 0 ? new BoundedLRUHashMap<String, Boolean>(cacheCapacity) : null;
    }
    
    @Override
    public boolean matches(String str) {
      if (cache == null) {
        return regex.reset(str).matches();
      }
      Boolean isMatch = cache.get(str);
      if (isMatch == null) {
        isMatch = regex.reset(str).matches();
        cache.put(str, isMatch); // cache it for later fast reuse
      }      
      return isMatch.booleanValue();
    }

  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class BoundedLRUHashMap<K,V> extends LinkedHashMap<K,V> {
    
    private final int capacity;

    private BoundedLRUHashMap(int capacity) {
      super(16, 0.5f, true);
      this.capacity = capacity;
    }
    
    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
      return size() > capacity;
    }
      
  } 


  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
//  private static final class LiteralExpression implements Expression {
//    
//    private final String pattern;
//    
//    public LiteralExpression(String pattern) {
//      this.pattern = pattern;
//    }
//    
//    @Override
//    public boolean matches(String str) {
//      return pattern.equals(str);
//    }
//
//  }

}
