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

import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.shaded.com.google.code.regexp.Pattern;

import com.typesafe.config.Config;

/**
 * Command that examines each string value in a given field and replaces each substring of the string value
 * that matches the given string literal or grok pattern with the given replacement.
 * 
 * This command also supports grok dictionaries in the same way as the {@link GrokBuilder}.
 */
public final class FindReplaceBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("findReplace");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new FindReplace(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class FindReplace extends AbstractFieldTransformCommand {

    private final Matcher matcher;
    private final String literalPattern;
    private final String replacement;
    private final boolean replaceFirst;
    
    public FindReplace(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      GrokDictionaries dict = new GrokDictionaries(config, getConfigs());
      String replacementStr = getConfigs().getString(config, "replacement");
      String pattern = getConfigs().getString(config, "pattern");
      if (getConfigs().getBoolean(config, "isRegex", false)) {
        Pattern regex = dict.compileExpression(pattern);
        this.matcher = regex.pattern().matcher("");
        replacementStr = regex.replaceProperties(replacementStr);
        this.literalPattern = null;
      } else {
        this.matcher = null;
        this.literalPattern = pattern;
      }
      this.replacement = replacementStr;
      this.replaceFirst = getConfigs().getBoolean(config, "replaceFirst", false);
      validateArguments();
    }
        
    @Override
    protected Object transformFieldValue(Object value) {
      String str = value.toString();
      String result;
      if (literalPattern != null) {
        int max = replaceFirst ? 1 : -1;
        result = StringUtils.replace(str, literalPattern, replacement, max);
      } else {
        matcher.reset(str);
        if (replaceFirst) {
          result = matcher.replaceFirst(replacement);
        } else {
          result = matcher.replaceAll(replacement);            
        }
      }
      return result;
    }
    
  }
 
  /* Copied from commons-lang-3.1 */
  /*
   * Licensed to the Apache Software Foundation (ASF) under one or more
   * contributor license agreements.  See the NOTICE file distributed with
   * this work for additional information regarding copyright ownership.
   * The ASF licenses this file to You under the Apache License, Version 2.0
   * (the "License"); you may not use this file except in compliance with
   * the License.  You may obtain a copy of the License at
   *
   *      http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
  private static final class StringUtils {
    
    /**
     * <p>Replaces a String with another String inside a larger String,
     * for the first {@code max} values of the search String.</p>
     *
     * <p>A {@code null} reference passed to this method is a no-op.</p>
     *
     * <pre>
     * StringUtils.replace(null, *, *, *)         = null
     * StringUtils.replace("", *, *, *)           = ""
     * StringUtils.replace("any", null, *, *)     = "any"
     * StringUtils.replace("any", *, null, *)     = "any"
     * StringUtils.replace("any", "", *, *)       = "any"
     * StringUtils.replace("any", *, *, 0)        = "any"
     * StringUtils.replace("abaa", "a", null, -1) = "abaa"
     * StringUtils.replace("abaa", "a", "", -1)   = "b"
     * StringUtils.replace("abaa", "a", "z", 0)   = "abaa"
     * StringUtils.replace("abaa", "a", "z", 1)   = "zbaa"
     * StringUtils.replace("abaa", "a", "z", 2)   = "zbza"
     * StringUtils.replace("abaa", "a", "z", -1)  = "zbzz"
     * </pre>
     *
     * @param text  text to search and replace in, may be null
     * @param searchString  the String to search for, may be null
     * @param replacement  the String to replace it with, may be null
     * @param max  maximum number of values to replace, or {@code -1} if no maximum
     * @return the text with any replacements processed,
     *  {@code null} if null String input
     */
    public static String replace(String text, String searchString, String replacement, int max) {
        if (isEmpty(text) || isEmpty(searchString) || replacement == null || max == 0) {
            return text;
        }
        int start = 0;
        int end = text.indexOf(searchString, start);
        if (end == -1) {
            return text;
        }
        int replLength = searchString.length();
        int increase = replacement.length() - replLength;
        increase = increase < 0 ? 0 : increase;
        increase *= max < 0 ? 16 : max > 64 ? 64 : max;
        StringBuilder buf = new StringBuilder(text.length() + increase);
        while (end != -1) {
            buf.append(text, start, end).append(replacement);
            start = end + replLength;
            if (--max == 0) {
                break;
            }
            end = text.indexOf(searchString, start);
        }
        buf.append(text, start, text.length());
        return buf.toString();
    }

    private static boolean isEmpty(CharSequence cs) {
      return cs == null || cs.length() == 0;
    }

  }

}
