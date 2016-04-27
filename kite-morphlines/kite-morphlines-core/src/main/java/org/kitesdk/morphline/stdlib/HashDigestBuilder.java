
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

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Collections;
import java.util.ListIterator;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.google.common.base.Charsets;
import com.typesafe.config.Config;

/**
 * Command that creates a hash digest of a field so-as to deterministically
 * provide a unique key
 */

public class HashDigestBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("hashDigest");
  }

  @Override
  public Command build(Config config, Command parent, Command child,
      MorphlineContext context) {
    return new HashDigest(this, config, parent, child, context);
  }

  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class HashDigest extends AbstractCommand {

    private final String inputFieldName;
    private final String outputFieldName;
    private final String hashType;
    private final boolean preserveExisting;
    private final Charset charset;
    private final MessageDigest digest;

    private static final String INPUT_FIELD = "inputField";
    private static final String OUTPUT_FIELD = "outputField";
    private static final String HASH_TYPE = "hashType";
    private static final String PRESERVE_EXISTING_NAME = "preserveExisting";
    private static final String CHARSET_FIELD = "charset";
    private static final boolean PRESERVE_EXISTING_DEFAULT = true;

    public HashDigest(CommandBuilder builder, Config config, Command parent,
        Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);

      this.inputFieldName = getConfigs().getString(config, INPUT_FIELD);
      this.outputFieldName = getConfigs().getString(config, OUTPUT_FIELD);
      this.hashType = getConfigs().getString(config, HASH_TYPE);
      this.preserveExisting = getConfigs().getBoolean(config,
          PRESERVE_EXISTING_NAME, PRESERVE_EXISTING_DEFAULT);
      this.charset = getConfigs().getCharset(config, CHARSET_FIELD, Charsets.UTF_8);
      

      try {
        this.digest = MessageDigest.getInstance(hashType);
      } catch (NoSuchAlgorithmException e) {
        throw new MorphlineCompilationException("Unable to initialise digest", config, e);
      }
      
      validateArguments();

      if (LOG.isTraceEnabled()) {
        LOG.trace("inputField: {}", inputFieldName);
        LOG.trace("outputField: {}", outputFieldName);
        LOG.trace("hashType: {}", hashType);
        LOG.trace("preserveExisting: {}", preserveExisting );
      }
    }

    @Override
    protected boolean doProcess(Record record) {

      if (!preserveExisting) {
        record.removeAll(outputFieldName);
      }

      ListIterator iter = record.get(inputFieldName).listIterator();
      
      while (iter.hasNext()) {
        Object inputField = iter.next();
        byte[] inputFieldBytes;
        if (inputField == null ) {
          inputFieldBytes = null;
        } else if (inputField instanceof byte[]) {
          inputFieldBytes = (byte[])inputField;
        } else {
          inputFieldBytes = inputField.toString().getBytes(charset);
        }
        record.put(outputFieldName, doHash(inputFieldBytes));
      }

      // pass record to next command in chain:
      return super.doProcess(record);

    }

    private String doHash(byte[] inputBytes) {
      digest.reset();
      
      return Hex.encodeHexString(digest.digest(inputBytes));
    }

  }

  //Manually shaded subset from org.apache.commons.codec 1.8.
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
  private static class Hex {
    /**
     * Used to build output as Hex
     */
    private static final char[] DIGITS_LOWER =
        {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * Used to build output as Hex
     */
    private static final char[] DIGITS_UPPER =
        {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    
    protected static char[] encodeHex(final byte[] data, final char[] toDigits) {
      final int l = data.length;
      final char[] out = new char[l << 1];
      // two characters form the hex value.
      for (int i = 0, j = 0; i < l; i++) {
          out[j++] = toDigits[(0xF0 & data[i]) >>> 4];
          out[j++] = toDigits[0x0F & data[i]];
      }
      return out;
    }
    /**
     * Converts an array of bytes into a String representing the hexadecimal values of each byte in order. The returned
     * String will be double the length of the passed array, as it takes two characters to represent any given byte.
     *
     * @param data
     *            a byte[] to convert to Hex characters
     * @return A String containing hexadecimal characters
     * @since 1.4
     */
    public static String encodeHexString(final byte[] data) {
        return new String(encodeHex(data));
    }
    
    /**
     * Converts an array of bytes into an array of characters representing the hexadecimal values of each byte in order.
     * The returned array will be double the length of the passed array, as it takes two characters to represent any
     * given byte.
     *
     * @param data
     *            a byte[] to convert to Hex characters
     * @return A char[] containing hexadecimal characters
     */
    public static char[] encodeHex(final byte[] data) {
        return encodeHex(data, true);
    }
    
    /**
     * Converts an array of bytes into an array of characters representing the hexadecimal values of each byte in order.
     * The returned array will be double the length of the passed array, as it takes two characters to represent any
     * given byte.
     *
     * @param data
     *            a byte[] to convert to Hex characters
     * @param toLowerCase
     *            <code>true</code> converts to lowercase, <code>false</code> to uppercase
     * @return A char[] containing hexadecimal characters
     * @since 1.4
     */
    public static char[] encodeHex(final byte[] data, final boolean toLowerCase) {
        return encodeHex(data, toLowerCase ? DIGITS_LOWER : DIGITS_UPPER);
    }
    
  }
}
