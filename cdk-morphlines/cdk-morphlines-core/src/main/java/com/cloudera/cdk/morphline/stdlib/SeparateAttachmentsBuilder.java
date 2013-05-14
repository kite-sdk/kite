/**
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
package com.cloudera.cdk.morphline.stdlib;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Fields;
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
    return new SeparateAttachments(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class SeparateAttachments extends AbstractCommand {

    public SeparateAttachments(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
    }
        
    @Override
    protected boolean doProcess(Record record) {      
      List attachments = record.get(Fields.ATTACHMENT_BODY);
      List mimeTypes = record.get(Fields.ATTACHMENT_MIME_TYPE);
      List charsets = record.get(Fields.ATTACHMENT_CHARSET);
      List names = record.get(Fields.ATTACHMENT_NAME);
      for (int i = 0; i < attachments.size(); i++) {
        Record outputRecord = record.copy();        
        outputRecord.replaceValues(Fields.ATTACHMENT_BODY, attachments.get(i));
        
        List replacement;
        replacement = i < mimeTypes.size() ? Collections.singletonList(mimeTypes.get(i)) : Collections.EMPTY_LIST;
        outputRecord.getFields().replaceValues(Fields.ATTACHMENT_MIME_TYPE, replacement);
        
        replacement = i < charsets.size() ? Collections.singletonList(charsets.get(i)) : Collections.EMPTY_LIST;
        outputRecord.getFields().replaceValues(Fields.ATTACHMENT_CHARSET, replacement);
        
        replacement = i < names.size() ? Collections.singletonList(names.get(i)) : Collections.EMPTY_LIST;
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
