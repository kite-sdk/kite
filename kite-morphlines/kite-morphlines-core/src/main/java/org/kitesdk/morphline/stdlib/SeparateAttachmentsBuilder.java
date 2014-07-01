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
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;

import com.typesafe.config.Config;

/**
 * Command that emits one output record for each attachment in the input record's list of
 * attachments; The result is a list of records, each of which has at most one attachment.
 */
public final class SeparateAttachmentsBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("separateAttachments");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new SeparateAttachments(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class SeparateAttachments extends AbstractCommand {

    public SeparateAttachments(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {      
      List attachments = record.get(Fields.ATTACHMENT_BODY);
      List mimeTypes = record.get(Fields.ATTACHMENT_MIME_TYPE);
      List charsets = record.get(Fields.ATTACHMENT_CHARSET);
      List names = record.get(Fields.ATTACHMENT_NAME);
      for (int i = 0; i < attachments.size(); i++) {
        Record outputRecord = record.copy();        
        outputRecord.getFields().replaceValues(Fields.ATTACHMENT_BODY, Collections.singletonList(attachments.get(i)));
        
        List<Object> replacement;
        replacement = i < mimeTypes.size() ? Collections.singletonList(mimeTypes.get(i)) : Collections.emptyList();
        outputRecord.getFields().replaceValues(Fields.ATTACHMENT_MIME_TYPE, replacement);
        
        replacement = i < charsets.size() ? Collections.singletonList(charsets.get(i)) : Collections.emptyList();
        outputRecord.getFields().replaceValues(Fields.ATTACHMENT_CHARSET, replacement);
        
        replacement = i < names.size() ? Collections.singletonList(names.get(i)) : Collections.emptyList();
        outputRecord.getFields().replaceValues(Fields.ATTACHMENT_NAME, replacement);
        
        // pass record to next command in chain:
        if (!super.doProcess(outputRecord)) {
          return false;
        }
      }
      return true;
    }
    
  }
  
}
