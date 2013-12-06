/**
 * Copyright (C) 2012-2013 The named-regexp Authors
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
package org.kitesdk.morphline.shaded.com.google.code.regexp;

import java.util.List;
import java.util.Map;

/**
 * The result of a match operation.
 *
 * <p>This interface contains query methods used to determine the results of
 * a match against a regular expression. The match boundaries, groups and
 * group boundaries can be seen but not modified through a MatchResult.</p>
 *
 * @since 0.1.9
 */
public interface MatchResult extends java.util.regex.MatchResult {

    /**
     * Returns the named capture groups in order
     *
     * @return the named capture groups
     */
    public List<String> orderedGroups();

    /**
     * Returns the named capture groups
     *
     * @return the named capture groups
     */
    public Map<String, String> namedGroups();

    /**
     * Returns the input subsequence captured by the given group during the
     * previous match operation.
     *
     * @param groupName name of capture group
     * @return the subsequence
     */
    public String group(String groupName);

    /**
     * Returns the start index of the subsequence captured by the given group
     * during this match.
     *
     * @param groupName name of capture group
     * @return the index
     */
    public int start(String groupName);

    /**
     * Returns the offset after the last character of the subsequence captured
     * by the given group during this match.
     *
     * @param groupName name of capture group
     * @return the offset
     */
    public int end(String groupName);

}
