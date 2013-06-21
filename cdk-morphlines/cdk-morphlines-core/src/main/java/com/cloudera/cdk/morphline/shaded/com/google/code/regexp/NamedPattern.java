/**
 * Copyright (C) 2012 The named-regexp Authors
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
package com.cloudera.cdk.morphline.shaded.com.google.code.regexp;

/**
 * Provided as a transitional class to avoid renaming existing references
 *
 * @deprecated see {@link Pattern}
 */
@Deprecated
public final class NamedPattern extends Pattern {

    /**
     * Constructs a named pattern with the given regular expression and flags
     *
     * @param regex the expression to be compiled
     * @param flags Match flags, a bit mask that may include CASE_INSENSITIVE, MULTILINE, DOTALL, UNICODE_CASE, CANON_EQ, UNIX_LINES, LITERAL and COMMENTS
     */
    private NamedPattern(String regex, int flags) {
        super(regex, flags);
    }

    /**
     * Compiles the given regular expression into a pattern
     *
     * @param regex the expression to be compiled
     * @return the pattern
     */
    public static NamedPattern compile(String regex) {
        return new NamedPattern(regex, 0);
    }

    /**
     * Creates a matcher that will match the given input against this pattern.
     *
     * @param input The character sequence to be matched
     * @return A new matcher for this pattern
     */
    public NamedMatcher matcher(CharSequence input) {
        return new NamedMatcher(this, input);
    }
}
