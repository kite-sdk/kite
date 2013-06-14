/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.morphline.hadoop.sequencefile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Configs;
import com.cloudera.cdk.morphline.base.Fields;
import com.typesafe.config.Config;

/**
 * Command that does custom parsing of MyWritable fields into output record
 */
public final class ParseTextMyWritableBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("parseTextMyWritable");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ParseTextMyWritable(config, parent, child, context);
  }

  private static final class ParseTextMyWritable extends AbstractCommand {
    private final String keyField;
    private final String valueField;

    public ParseTextMyWritable(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      this.keyField = getConfigs().getString(config, ReadSequenceFileBuilder.CONFIG_KEY_FIELD, Fields.ATTACHMENT_BODY);
      this.valueField = getConfigs().getString(config, ReadSequenceFileBuilder.CONFIG_VALUE_FIELD, Fields.ATTACHMENT_BODY);
    }

    @Override
    protected boolean doProcess(Record inputRecord) {
      Record outputRecord = inputRecord.copy();

      // change key
      Text myTextKey = (Text)inputRecord.getFirstValue(this.keyField);
      outputRecord.replaceValues(this.keyField, MyWritable.keyStr(myTextKey));
      // change value
      MyWritable myWritableValue = (MyWritable)inputRecord.getFirstValue(this.valueField);
      outputRecord.replaceValues(this.valueField, MyWritable.valueStr(myWritableValue));
      return super.doProcess(outputRecord);
    }
  }

  /**
   * Writable derived class for testing.
   */
  public static class MyWritable implements WritableComparable {
    private String prefix;
    private int suffix;

    /**
     * Empty constructor for Writable
     */
    public MyWritable() {
    }

    public MyWritable(String prefix, int suffix) {
      this.prefix = prefix;
      this.suffix = suffix;
    }

    public void readFields(DataInput in) throws IOException {
      this.prefix = in.readUTF();
      this.suffix = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
      out.writeUTF(this.prefix);
      out.writeInt(this.suffix);
    }

    public int compareTo(Object o) {
      throw new NotImplementedException("not implemented!");
    }
  
    public String getPrefix() { return prefix; }
    public int getSuffix() { return suffix; }


    public static String keyStr(Text key) {
      // do something recognizable
      return key.toString().toUpperCase();
    }
    public static String valueStr(MyWritable value) {
      // do something recognizable
      return value.getSuffix() + value.getPrefix();
    }
  }
}
