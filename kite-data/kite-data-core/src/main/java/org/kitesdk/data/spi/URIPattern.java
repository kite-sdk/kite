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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Defines a URI pattern that can be used to parse and match other URIs.
 *
 * @since 0.8.0
 */
public class URIPattern {

  public static final String SCHEME = "uri:scheme";
  public static final String HOST = "auth:host";
  public static final String PORT = "auth:port";
  public static final String USERNAME = "auth:username";
  public static final String PASSWORD = "auth:password";

  private static final Splitter PATH_SPLITTER = Splitter.on('/');
  private static final Joiner PATH_JOINER = Joiner.on('/');
  private static final Splitter PATH_QUERY_SPLITTER = Splitter.on('?');
  private static final Splitter.MapSplitter QUERY_SPLITTER =
      Splitter.on('&').withKeyValueSeparator(Splitter.on('='));
  private static final Joiner.MapJoiner QUERY_JOINER =
      Joiner.on('&').withKeyValueSeparator("=");

  private static final CharMatcher USER_PASS_SEPARATOR = CharMatcher.is(':');
  private static final CharMatcher VAR_START = CharMatcher.anyOf(":*");

  private static final int UNSPECIFIED_PORT = -1;

  private URI pattern;
  private String patternPath;
  private Map<String, String> defaults;
  private Map<String, String> lastMatch;

  public URIPattern(URI uri) {
    this.pattern = uri;

    Map<String, String> accumulator = Maps.newHashMap();
    if (pattern.isOpaque()) {
      Iterator<String> pathQuery = PATH_QUERY_SPLITTER
          .split(pattern.getSchemeSpecificPart()).iterator();
      this.patternPath = Iterators.getNext(pathQuery, null);
      addQuery(Iterators.getNext(pathQuery, null), accumulator);
    } else {
      patternPath = pattern.getRawPath();
      addQuery(pattern.getRawQuery(), accumulator);
      addAuthority(pattern, accumulator);
    }
    if (pattern.getScheme() != null) {
      accumulator.put(SCHEME, pattern.getScheme());
    }
    this.defaults = ImmutableMap.copyOf(accumulator);
  }

  public URIPattern(String uri) {
    this(URI.create(uri));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    URIPattern that = (URIPattern) o;
    return Objects.equal(this.pattern, that.pattern);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(pattern);
  }

  /**
   * Constructs a {@link URI} that matches this pattern from the given options.
   *
   * @param options a map of variable and
   * @return a {@link URI} that matches this pattern
   * @throws IllegalArgumentException if the URI cannot be constructed
   */
  public URI construct(Map<String, String> options) {
    // make a copy that can be modified
    Map<String, String> uriData = Maps.newLinkedHashMap(options);
    try {
      // scheme should always be present, but default if necessary
      String scheme = defaults.get(SCHEME);
      if (uriData.containsKey(SCHEME)) {
        scheme = uriData.remove(SCHEME);
      }

      StringBuilder builder = new StringBuilder();
      builder.append(scheme).append(":");

      if (!pattern.isOpaque()) {
        // Build the URI(String,String,String,int,String,String,String)
        StringBuilder authBuilder = new StringBuilder();
        String user = constructUserInfo(uriData, defaults);
        String host = removeNonDefault(HOST, uriData, defaults);
        int port = constructPort(uriData, defaults);
        if (user != null) {
          authBuilder.append(user).append("@");
        }
        if (host != null) {
          authBuilder.append(host);
        }
        if (port >= 0) {
          authBuilder.append(":").append(port);
        }
        String auth = authBuilder.toString();
        if (!auth.isEmpty()) {
          builder.append("//").append(auth);
        }
      }

      builder.append(constructPath(uriData, patternPath));

      String query = constructQuery(uriData, defaults);
      if (query != null) {
        builder.append("?").append(query);
      }

      return new URI(builder.toString());

    } catch (URISyntaxException ex) {
      throw new IllegalArgumentException("Could not build URI", ex);
    }
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

    Map<String, String> result = Maps.newLinkedHashMap(defaults);

    if (pattern.isOpaque()) {
      if (!uri.isOpaque()) {
        return null;
      }

      Iterator<String> pathQuery = PATH_QUERY_SPLITTER
          .split(uri.getRawSchemeSpecificPart()).iterator();

      if (!addPath(patternPath, Iterators.getNext(pathQuery, null), result)) {
        return null;
      }

      addQuery(Iterators.getNext(pathQuery, null), result);

    } else if (!uri.isOpaque()) {
      addAuthority(uri, result);

      if (patternPath.isEmpty() && !uri.getRawPath().isEmpty()) {
        return null;
      }

      if (!addPath(patternPath, uri.getRawPath(), result)) {
        return null;
      }

      addQuery(uri.getRawQuery(), result);

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

  private static boolean addPath(String pattern, String path,
                                 Map<String, String> storage) {
    boolean matchedAfterGlob = false;
    boolean matchedBeforeGlob = false;
    LinkedList<String> patternParts = Lists.newLinkedList(
        PATH_SPLITTER.split(pattern));
    LinkedList<String> parts = Lists.newLinkedList(PATH_SPLITTER.split(path));

    // consume URI parts moving forward until exhausted or a glob pattern
    String globPattern = null;
    while (!patternParts.isEmpty()) {
      String patternPart = patternParts.removeFirst();
      if (patternPart.startsWith("*")) {
        globPattern = patternPart;
        break;
      }
      if (parts.isEmpty() ||
          !addComplexMatch(patternPart, parts.removeFirst(), storage)) {
        // abort if the pattern doesn't account for an entry
        return false;
      }
      if (!patternPart.isEmpty()) {
        matchedBeforeGlob = true;
      }
    }

    // consume URI parts moving backward until exhausted
    while (!patternParts.isEmpty()) {
      String patternPart = patternParts.removeLast();
      if (patternPart.startsWith("*")) {
        // globPattern must be non-null because patternParts is not empty
        throw new RuntimeException("Cannot use multiple glob patterns");
      }
      if (parts.isEmpty() ||
          !addComplexMatch(patternPart, parts.removeLast(), storage)) {
        // abort if the pattern doesn't account for an entry
        return false;
      }
      matchedAfterGlob = true;
    }

    // see if there was a glob that matched
    if (globPattern != null && !parts.isEmpty()) {
      final String joined = PATH_JOINER.join(parts);
      if (!joined.isEmpty() || matchedAfterGlob || matchedBeforeGlob) {
        storage.put(globPattern.substring(1), joined);
      }
      return true; // all remaining parts are consumed
    }

    // only successful if there are no unmatched parts
    return parts.isEmpty();
  }

  private static boolean addComplexMatch(
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

  private static boolean addSimpleMatch(
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

  private static void addQuery(String query, Map<String, String> storage) {
    if (query != null) {
      storage.putAll(QUERY_SPLITTER.split(query));
    }
  }

  private static String removeNonDefault(String key,
                                         Map<String, String> options,
                                         Map<String, String> defaults) {
    String defaultValue = defaults.get(key);
    if (options.containsKey(key)) {
      String value = options.remove(key);
      if (defaultValue == null || !defaultValue.equals(value)) {
        return value;
      }
    }
    // return null to default the value when parsed
    return null;
  }

  private static String constructQuery(Map<String, String> uriOptions,
                                       Map<String, String> defaults) {
    // remove any default values
    for (Map.Entry<String, String> entry : defaults.entrySet()) {
      String key = entry.getKey();
      if (uriOptions.containsKey(key)) {
        String defaultValue = entry.getValue();
        if (defaultValue != null && defaultValue.equals(uriOptions.get(key))) {
          uriOptions.remove(key); // discard default value
        }
      }
    }
    if (uriOptions.isEmpty()) {
      return null;
    }
    return QUERY_JOINER.join(uriOptions);
  }

  private static String constructPath(Map<String, String> match,
                                      String pattern) {
    LinkedList<String> patternParts = Lists.newLinkedList(
        PATH_SPLITTER.split(pattern));
    List<String> pathParts = Lists
        .newArrayListWithExpectedSize(patternParts.size());
    for (String part : patternParts) {
      if (!part.isEmpty()) {
        // only supports simple matches, like matching
        if (VAR_START.matches(part.charAt(0))) {
          String name = part.substring(1);
          if (match.containsKey(name)) {
            pathParts.add(match.remove(name));
          } else if (!part.startsWith("*")) {
            // required variable is missing
            throw new IllegalArgumentException(
                "Missing required option: " + name);
          }
        } else {
          // it is a required URI part
          pathParts.add(part);
        }
      } else {
        // preserve additional slashes in paths
        pathParts.add(part);
      }
    }
    return PATH_JOINER.join(pathParts);
  }

  private static int constructPort(Map<String, String> uriOptions,
                                   Map<String, String> defaults) {
    String portStr = removeNonDefault(PORT, uriOptions, defaults);
    if (portStr != null) {
      try {
        return Integer.parseInt(portStr);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid port: " + portStr, e);
      }
    }
    return UNSPECIFIED_PORT;
  }

  private static String constructUserInfo(Map<String, String> uriOptions,
                                          Map<String, String> defaults) {
    String username = removeNonDefault(USERNAME, uriOptions, defaults);
    String password = removeNonDefault(PASSWORD, uriOptions, defaults);
    if (username != null) {
      if (password != null) {
        return username + ":" + password;
      }
      return username;
    }
    return null;
  }
}
