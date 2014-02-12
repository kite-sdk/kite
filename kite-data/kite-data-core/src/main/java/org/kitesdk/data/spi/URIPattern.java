/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

/**
 * Defines a URI pattern that can be used to parse and match other URIs.
 *
 * @since 0.8.0
 */
public class URIPattern {

  public static final String SCHEME = "scheme";
  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";

  private static final Splitter PATH_SPLITTER = Splitter.on('/');
  private static final Joiner PATH_JOINER = Joiner.on('/');
  private static final Splitter.MapSplitter QUERY_SPLITTER =
      Splitter.on('&').withKeyValueSeparator(Splitter.on('='));

  private static final CharMatcher USER_PASS_SEPARATOR = CharMatcher.is(':');
  private static final CharMatcher VAR_START = CharMatcher.anyOf(":*");

  private URI pattern;
  private Map<String, String> defaults;
  private Map<String, String> lastMatch;

  public URIPattern(URI uri) {
    this.pattern = uri;

    Map<String, String> accumulator = Maps.newHashMap();
    addQuery(pattern, accumulator);
    addAuthority(pattern, accumulator);
    if (pattern.getScheme() != null) {
      accumulator.put(SCHEME, pattern.getScheme());
    }
    this.defaults = ImmutableMap.copyOf(accumulator);
  }

  public URIPattern(String uri) throws URISyntaxException {
    this(new URI(uri));
  }

  /**
   * Returns true if the {@link java.net.URI} matches this pattern.
   *
   * If successful, the variable values are available by calling
   * {@link #getLastMatch()} (not thread safe).
   *
   * @param uri a String to check against this pattern.
   * @return true if the String matches this pattern
   */
  public boolean matches(URI uri) {
    return (getMatch(uri) != null);
  }

  /**
   * Returns true if the {@link java.net.URI} String matches this pattern.
   *
   * If successful, the variable values are available by calling
   * {@link #getLastMatch()} (not thread safe).
   *
   * @param uri a String to check against this pattern.
   * @return true if the String matches this pattern
   * @throws URISyntaxException if the String is not a valid URI
   */
  public boolean matches(String uri) throws URISyntaxException {
    return matches(new URI(uri));
  }

  /**
   * Returns results from parsing a {@link java.net.URI} with this pattern.
   * If the URI doesn't match the pattern, this will return null.
   *
   * @param uri A URI to match against this URIPattern
   * @return A Map of the pattern's variable names to values from the parsed URI
   */
  public Map<String, String> getMatch(URI uri) {
    // verify that the schemes match
    if (pattern.isAbsolute()) {
      // if there should be a scheme, make sure it matches
      if (!pattern.getScheme().equalsIgnoreCase(uri.getScheme())) {
        return null;
      }
    } else if (uri.getScheme() != null) {
      return null;
    }

    Map<String, String> result = Maps.newHashMap(defaults);

    if (pattern.isOpaque()) {
      addComplexMatch(
          pattern.getSchemeSpecificPart(),
          uri.getSchemeSpecificPart(),
          result);

    } else if (!uri.isOpaque()) {
      addAuthority(uri, result);

      if (pattern.getPath().isEmpty() && !uri.getPath().isEmpty()) {
        return null;
      }

      Iterator<String> parts = PATH_SPLITTER.split(uri.getPath()).iterator();
      for (String patternPart : PATH_SPLITTER.split(pattern.getPath())) {
        if (!addMatch(patternPart, parts, result)) {
          // abort if the pattern doesn't account for an entry
          return null;
        }
      }

      addQuery(uri, result);
    } else {
      return null;
    }

    if (!addComplexMatch(pattern.getFragment(), uri.getFragment(), result)) {
      return null;
    }

    // save this match
    this.lastMatch = result;

    // return the new result, so that this is thread-safe
    return result;
  }

  /**
   * Returns results from parsing a {@link java.net.URI} with this pattern.
   * If the URI doesn't match the pattern, this will return null.
   *
   * @param uri A URI to match against this URIPattern
   * @return A Map of the pattern's variable names to values from the parsed URI
   * @throws URISyntaxException if the String is not a valid URI
   */
  public Map<String, String> getMatch(String uri) throws URISyntaxException {
    return getMatch(new URI(uri));
  }

  /**
   * Returns the parsed results of the last matched URI. Not thread-safe.
   *
   * This is intended as a convenience for single-threaded callers that want to
   * use matches() and not re-parse the URI to get the result.
   *
   * @return a result map from the last successful match
   */
  public Map<String, String> getLastMatch() {
    return lastMatch;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(URIPattern.class)
        .add("pattern", pattern)
        .toString();
  }

  private boolean addMatch(
      String pattern, Iterator<String> parts, Map<String, String> storage) {
    // this assumes pattern will be (?:\:\w+\??)|(?:\*\w+)
    if (pattern.startsWith("*")) {
      final String joined = PATH_JOINER.join(parts);
      if (!joined.isEmpty()) {
        storage.put(pattern.substring(1), joined);
      }
      return true;
    } else if (pattern.endsWith("?")) {
      // TODO: this doesn't currently hit because the ? is consumed by query
      if (parts.hasNext()) {
        return addComplexMatch(
            pattern.substring(0, pattern.length()-1), parts.next(), storage);
      } else {
        return true;
      }
    } else if (parts.hasNext()) {
      return addComplexMatch(pattern, parts.next(), storage);
    }
    return false;
  }

  private boolean addComplexMatch(
      String pattern, String part, Map<String, String> storage) {
    // TODO: extend this to match more complicated patterns, like:
    // ":doc.:fmt?" and "index.html" => {"doc" => "index", "fmt" => "html"}
    // ":doc.:fmt?" and "index" => {"doc" => "index"}
    // ":user@:domain" etc.

    // This will also support opaque URIs better
    if (pattern == null) {
      return (part == null);
    } else if (pattern.isEmpty()) {
      return part.isEmpty();
    } else {
      // This imposes no restrictions on variable naming
      return addSimpleMatch(pattern, part, storage);
    }
  }

  private boolean addSimpleMatch(
      String pattern, String part, Map<String, String> storage) {
    if (VAR_START.matches(pattern.charAt(0))) {
      storage.put(pattern.substring(1), part);
      return true;
    } else {
      // not a captured part, verify it matches the pattern
      return pattern.equals(part);
    }
  }

  private static void addAuthority(URI uri, Map<String, String> storage) {
    if (uri.getHost() != null) {
      storage.put(HOST, uri.getHost());
    }
    if (uri.getPort() >= 0) {
      storage.put(PORT, String.valueOf(uri.getPort()));
    }

    final String userInfo = uri.getUserInfo();
    if (userInfo != null) {
      // the first colon is the delimiter between the username and the password
      int index = USER_PASS_SEPARATOR.indexIn(userInfo);
      if (index > 0) {
        storage.put(USERNAME, userInfo.substring(0, index));
        storage.put(PASSWORD, userInfo.substring(index+1));
      } else {
        storage.put(USERNAME, userInfo);
      }
    }
  }

  private static void addQuery(URI uri, Map<String, String> storage) {
    String query = uri.getQuery();
    if (query != null) {
      storage.putAll(QUERY_SPLITTER.split(query));
    }
  }
}
