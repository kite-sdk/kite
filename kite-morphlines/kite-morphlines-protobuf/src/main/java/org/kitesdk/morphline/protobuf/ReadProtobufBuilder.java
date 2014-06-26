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
package org.kitesdk.morphline.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.typesafe.config.Config;

/**
 * Command that parses an InputStream that contains Protocol buffer data; the
 * command emits a morphline record containing the object as an attachment in
 * {@link Fields#ATTACHMENT_BODY}.
 */
public final class ReadProtobufBuilder implements CommandBuilder {

  /** The MIME type identifier that will be filled into output records */
  public static final String MIME_TYPE = "application/x-protobuf";

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadProtobuf(this, config, parent, child, context);
  }

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readProtobuf");
  }

  // /////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  // /////////////////////////////////////////////////////////////////////////////
  private static final class ReadProtobuf extends AbstractParser {

    private static final String PARSE_FROM = "parseFrom";
    private final Class<?> outputClass;
    private final Class<?> protobufClass;
    private final Method parseMethod;

    public ReadProtobuf(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);

      String protoClassName = getConfigs().getString(config, "protobufClass");
      try {
        protobufClass = Class.forName(protoClassName);
      } catch (ClassNotFoundException e) {
        throw new MorphlineCompilationException("Class not found", config, e);
      }

      String outputClassName = getConfigs().getString(config, "outputClass");
      outputClass = getInnerClass(protobufClass, outputClassName);
      if (outputClass == null) {
        throw new MorphlineCompilationException("Output class '" + outputClassName + "' does not exist in class '"
            + protoClassName + "'.", config);
      }

      try {
        parseMethod = outputClass.getMethod(PARSE_FROM, InputStream.class);
      } catch (NoSuchMethodException e) {
        throw new MorphlineCompilationException("Method '" + PARSE_FROM + "' does not exist in class '" + outputClassName
            + "'.", config, e);
      } catch (SecurityException e) {
        throw new MorphlineCompilationException("Method '" + PARSE_FROM + "' is probably not public in class '"
            + outputClassName + "'.", config, e);
      }

      validateArguments();
    }

    @Override
    protected boolean doProcess(Record inputRecord, InputStream in) throws IOException {

      Object protoObjectInstance;
      try {
        protoObjectInstance = parseMethod.invoke(null, in);
      } catch (Exception e) {
        throw new IOException("Instance creation of '" + outputClass.getName() + "' from inputStream failed. "
            + e.getMessage(), e);
      }

      incrementNumRecords();
      LOG.trace("protoObject: {}", protoObjectInstance);

      Record outputRecord = inputRecord.copy();
      removeAttachments(outputRecord);
      outputRecord.put(Fields.ATTACHMENT_BODY, protoObjectInstance);
      outputRecord.put(Fields.ATTACHMENT_MIME_TYPE, MIME_TYPE);

      // pass record to next command in chain:
      if (!getChild().process(outputRecord)) {
        return false;
      }

      return true;
    }

    private Class<?> getInnerClass(Class<?> declaringClass, String innerClassName) {
      Class<?> innerClass = null;
      for (Class<?> class1 : protobufClass.getDeclaredClasses()) {
        if (class1.getSimpleName().equals(innerClassName)) {
          innerClass = class1;
        }
      }
      return innerClass;
    }
  }
}
