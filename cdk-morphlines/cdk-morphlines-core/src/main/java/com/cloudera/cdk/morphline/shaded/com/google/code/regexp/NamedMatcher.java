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
 * @deprecated see {@link Matcher}
 */
@Deprecated
public final class NamedMatcher extends Matcher {

    NamedMatcher(Pattern parentPattern, java.util.regex.MatchResult matcher) {
        super(parentPattern, matcher);
    }

    NamedMatcher(Pattern parentPattern, CharSequence input) {
        super(parentPattern, input);
    }
}
