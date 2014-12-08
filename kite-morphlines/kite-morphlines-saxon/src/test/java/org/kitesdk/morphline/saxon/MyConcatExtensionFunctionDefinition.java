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
package org.kitesdk.morphline.saxon;

import net.sf.saxon.expr.XPathContext;
import net.sf.saxon.lib.ExtensionFunctionCall;
import net.sf.saxon.lib.ExtensionFunctionDefinition;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.om.StructuredQName;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.SequenceType;
import net.sf.saxon.value.StringValue;

/** Saxon Full API for Java extension functions */
public class MyConcatExtensionFunctionDefinition extends ExtensionFunctionDefinition {
  
  @Override
  public StructuredQName getFunctionQName() {
    return new StructuredQName("myfn", "my.custom.uri", "my-concat-def");
  }

  @Override
  public SequenceType[] getArgumentTypes() {
    return new SequenceType[] { SequenceType.ANY_SEQUENCE };
  }

  @Override
  public SequenceType getResultType(SequenceType[] sequenceTypes) {
    return SequenceType.SINGLE_STRING;
  }

  @Override
  public ExtensionFunctionCall makeCallExpression() {
    return new ExtensionFunctionCall() {
      @Override
      public Sequence call(XPathContext ctx, Sequence[] secs) throws XPathException {
        if (secs.length == 0) {
          throw new XPathException("Missing argument");
        } else {
          StringBuilder buf = new StringBuilder();
          for (Sequence seq : secs) {
            SequenceIterator<? extends Item> iter = seq.iterate();
            while (iter.next() != null) {
              buf.append(iter.current().getStringValue());
            }
          }
          return new StringValue(buf.toString());
        }
      }
    };
  }
}
