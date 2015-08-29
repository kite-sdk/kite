/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.filesystem;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang.StringEscapeUtils;
import org.kitesdk.data.DatasetDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class CSVProperties {
  private static final Logger LOG = LoggerFactory
      .getLogger(CSVProperties.class);

  public static final String CHARSET_PROPERTY = "kite.csv.charset";
  public static final String DELIMITER_PROPERTY = "kite.csv.delimiter";
  public static final String QUOTE_CHAR_PROPERTY = "kite.csv.quote-char";
  public static final String ESCAPE_CHAR_PROPERTY = "kite.csv.escape-char";
  public static final String HEADER_PROPERTY = "kite.csv.header";
  public static final String HAS_HEADER_PROPERTY = "kite.csv.has-header";
  public static final String LINES_TO_SKIP_PROPERTY = "kite.csv.lines-to-skip";

  // old properties
  public static final String OLD_CHARSET_PROPERTY = "cdk.csv.charset";
  public static final String OLD_DELIMITER_PROPERTY = "cdk.csv.delimiter";
  public static final String OLD_QUOTE_CHAR_PROPERTY = "cdk.csv.quote-char";
  public static final String OLD_ESCAPE_CHAR_PROPERTY = "cdk.csv.escape-char";
  public static final String OLD_LINES_TO_SKIP_PROPERTY = "cdk.csv.lines-to-skip";

  public static final String DEFAULT_CHARSET = "utf8";
  public static final String DEFAULT_DELIMITER = ",";
  public static final String DEFAULT_QUOTE = "\"";
  public static final String DEFAULT_ESCAPE = "\\";
  public static final String DEFAULT_HAS_HEADER = "false";
  public static final int DEFAULT_LINES_TO_SKIP = 0;

  // configuration
  public final String charset;
  public final String delimiter;
  public final String quote;
  public final String escape;
  public final String header;
  public final boolean useHeader;
  public final int linesToSkip;

  private CSVProperties(String charset, String delimiter, String quote,
                        String escape, String header, boolean useHeader,
                        int linesToSkip) {
    this.charset = charset;
    this.delimiter = delimiter;
    this.quote = quote;
    this.escape = escape;
    this.header = header;
    this.useHeader = useHeader;
    this.linesToSkip = linesToSkip;
  }

  private CSVProperties(DatasetDescriptor descriptor) {
    this.charset = coalesce(
        descriptor.getProperty(CHARSET_PROPERTY),
        descriptor.getProperty(OLD_CHARSET_PROPERTY),
        DEFAULT_CHARSET);
    this.delimiter= coalesce(
        descriptor.getProperty(DELIMITER_PROPERTY),
        descriptor.getProperty(OLD_DELIMITER_PROPERTY),
        DEFAULT_DELIMITER);
    this.quote = coalesce(
        descriptor.getProperty(QUOTE_CHAR_PROPERTY),
        descriptor.getProperty(OLD_QUOTE_CHAR_PROPERTY),
        DEFAULT_QUOTE);
    this.escape = coalesce(
        descriptor.getProperty(ESCAPE_CHAR_PROPERTY),
        descriptor.getProperty(OLD_ESCAPE_CHAR_PROPERTY),
        DEFAULT_ESCAPE);
    this.header = descriptor.getProperty(HEADER_PROPERTY);
    this.useHeader = Boolean.parseBoolean(coalesce(
        descriptor.getProperty(HAS_HEADER_PROPERTY),
        DEFAULT_HAS_HEADER));
    final String linesToSkipString = coalesce(
        descriptor.getProperty(LINES_TO_SKIP_PROPERTY),
        descriptor.getProperty(OLD_LINES_TO_SKIP_PROPERTY));
    int lines = DEFAULT_LINES_TO_SKIP;
    if (linesToSkipString != null) {
      try {
        lines = Integer.parseInt(linesToSkipString);
      } catch (NumberFormatException ex) {
        LOG.debug("Defaulting lines to skip, failed to parse: {}",
            linesToSkipString);
        // lines remains set to the default
      }
    }
    this.linesToSkip = lines;
  }

  /**
   * Returns the first non-null value from the sequence or null if there is no
   * non-null value.
   */
  private static <T> T coalesce(T... values) {
    for (T value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public DatasetDescriptor addToDescriptor(DatasetDescriptor descriptor) {
    DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder(descriptor)
        .property(CHARSET_PROPERTY, charset)
        .property(DELIMITER_PROPERTY, delimiter)
        .property(ESCAPE_CHAR_PROPERTY, escape)
        .property(QUOTE_CHAR_PROPERTY, quote)
        .property(HAS_HEADER_PROPERTY, Boolean.toString(useHeader))
        .property(LINES_TO_SKIP_PROPERTY, Integer.toString(linesToSkip));

    if (header != null) {
      builder.property(HEADER_PROPERTY, header);
    }

    return builder.build();
  }

  public static CSVProperties fromDescriptor(DatasetDescriptor descriptor) {
    return new CSVProperties(descriptor);
  }

  public static class Builder {
    private String charset = DEFAULT_CHARSET;
    private String delimiter = DEFAULT_DELIMITER;
    private String quote = DEFAULT_QUOTE;
    private String escape = DEFAULT_ESCAPE;
    private boolean useHeader = Boolean.valueOf(DEFAULT_HAS_HEADER);
    private int linesToSkip = DEFAULT_LINES_TO_SKIP;
    private String header = null;

    public Builder charset(String charset) {
      this.charset = charset;
      return this;
    }

    public Builder delimiter(String delimiter) {
      this.delimiter = StringEscapeUtils.unescapeJava(delimiter);
      return this;
    }

    public Builder quote(String quote) {
      this.quote = StringEscapeUtils.unescapeJava(quote);
      return this;
    }

    public Builder escape(String escape) {
      this.escape = StringEscapeUtils.unescapeJava(escape);
      return this;
    }

    public Builder header(@Nullable String header) {
      this.header = header;
      return this;
    }

    public Builder hasHeader() {
      this.useHeader = true;
      return this;
    }

    public Builder hasHeader(boolean hasHeader) {
      this.useHeader = hasHeader;
      return this;
    }

    public Builder linesToSkip(int linesToSkip) {
      this.linesToSkip = linesToSkip;
      return this;
    }

    public CSVProperties build() {
      return new CSVProperties(
          charset, delimiter, quote, escape,
          header, useHeader, linesToSkip);
    }
  }
}
