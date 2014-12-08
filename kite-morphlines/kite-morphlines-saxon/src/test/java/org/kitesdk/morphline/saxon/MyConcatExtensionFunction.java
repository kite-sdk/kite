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

import net.sf.saxon.s9api.ExtensionFunction;
import net.sf.saxon.s9api.ItemType;
import net.sf.saxon.s9api.OccurrenceIndicator;
import net.sf.saxon.s9api.QName;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.SequenceType;
import net.sf.saxon.s9api.XdmAtomicValue;
import net.sf.saxon.s9api.XdmSequenceIterator;
import net.sf.saxon.s9api.XdmValue;

/** Saxon Simple API for integrated Java extension functions */
public class MyConcatExtensionFunction implements ExtensionFunction {
  
  @Override
  public QName getName() {
    return new QName("my.custom.uri", "my-concat");
  }

  @Override
  public SequenceType getResultType() {
    return SequenceType.makeSequenceType(
        ItemType.STRING, OccurrenceIndicator.ONE
    );
  }
  
  @Override
  public SequenceType[] getArgumentTypes() {
    return new SequenceType[]{
        SequenceType.makeSequenceType(
            ItemType.ANY_ITEM, OccurrenceIndicator.ONE_OR_MORE)};
  }
  
  @Override
  public XdmValue call(XdmValue[] arguments) throws SaxonApiException {
    StringBuffer buf = new StringBuffer();
    for (XdmValue seq : arguments) {
      XdmSequenceIterator iter = seq.iterator();
      while (iter.hasNext()) {
        buf.append(iter.next().getStringValue());
      }
    }
    return new XdmAtomicValue(buf.toString());
  }
}
