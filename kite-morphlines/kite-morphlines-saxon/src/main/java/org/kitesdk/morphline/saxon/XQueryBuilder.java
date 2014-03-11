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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import net.sf.saxon.s9api.Axis;
import net.sf.saxon.s9api.QName;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XQueryCompiler;
import net.sf.saxon.s9api.XQueryEvaluator;
import net.sf.saxon.s9api.XQueryExecutable;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmNodeKind;
import net.sf.saxon.s9api.XdmSequenceIterator;
import net.sf.saxon.s9api.XdmValue;
import net.sf.saxon.trace.XQueryTraceListener;
import net.sf.saxon.value.UntypedAtomicValue;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Configs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Command that parses an InputStream that contains an XML document and runs the given XQuery over
 * the XML document. For each item in the query result sequence, the command emits a morphline record
 * containing the item's name-value pairs.
 */
public final class XQueryBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("xquery");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    try {
      return new XQuery(this, config, parent, child, context);
    } catch (SaxonApiException e) {
      throw new MorphlineCompilationException("Cannot compile", config, e);
    } catch (IOException e) {
      throw new MorphlineCompilationException("Cannot compile", config, e);
    } catch (XMLStreamException e) {
      throw new MorphlineCompilationException("Cannot compile", config, e);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class XQuery extends SaxonCommand {
    
    /*
     * TODO: Add option to support streaming via fragmentPath.
     * 
     * TODO: Add option to support serializing each item in the result sequence according to the XML
     * Output Method of the <a target="_blank" href="http://www.w3.org/TR/xslt-xquery-serialization-30/">
     * W3C XQuery/XSLT2 Serialization Spec</a>, with sequence normalization as defined therein.
     */
    
    private final List<Fragment> fragments = new ArrayList();
  
    public XQuery(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) throws SaxonApiException, IOException, XMLStreamException {
      super(builder, config, parent, child, context);
      
      List<? extends Config> fragmentConfigs = getConfigs().getConfigList(config, "fragments");
      if (fragmentConfigs.size() == 0) {
        throw new MorphlineCompilationException("At least one fragment must be defined", config);
      }
      if (fragmentConfigs.size() > 1) {
        throw new MorphlineCompilationException("More than one fragment is not yet supported", config);
      }
      for (Config fragment : fragmentConfigs) {
        String fragmentPath = getConfigs().getString(fragment, "fragmentPath");
        if (!fragmentPath.equals("/")) {
          throw new MorphlineCompilationException("Non-root fragment paths are not yet supported", config);
        }  
        
        XQueryCompiler compiler = processor.newXQueryCompiler();
        compiler.setErrorListener(new DefaultErrorListener());
        compiler.setCompileWithTracing(isTracing);
        compiler.setLanguageVersion(getConfigs().getString(config, "languageVersion", "1.0"));
        
        XQueryExecutable executable = null;
        String query = getConfigs().getString(fragment, "queryString", null);
        if (query != null) {
          executable = compiler.compile(query);     
        }
        String queryFile = getConfigs().getString(fragment, "queryFile", null);
        if (queryFile != null) {
          executable = compiler.compile(new File(queryFile));
        }
        if (query == null && queryFile == null) {
          throw new MorphlineCompilationException("Either query or queryFile must be defined", config);
        }
        if (query != null && queryFile != null) {
          throw new MorphlineCompilationException("Must not define both query and queryFile at the same time", config);
        }
        
        XQueryEvaluator evaluator = executable.load();
        Config variables = getConfigs().getConfig(fragment, "externalVariables", ConfigFactory.empty());
        for (Map.Entry<String, Object> entry : new Configs().getEntrySet(variables)) {
          XdmValue xdmValue = XdmNode.wrap(new UntypedAtomicValue(entry.getValue().toString()));
          evaluator.setExternalVariable(new QName(entry.getKey()), xdmValue);
        }
        Config fileVariables = getConfigs().getConfig(fragment, "externalFileVariables", ConfigFactory.empty());
        for (Map.Entry<String, Object> entry : new Configs().getEntrySet(fileVariables)) {
          File file = new File(entry.getValue().toString());
          XdmValue doc = parseXmlDocument(file);
          evaluator.setExternalVariable(new QName(entry.getKey()), doc);
        }
        if (isTracing) {
          evaluator.setTraceListener(new XQueryTraceListener()); // TODO redirect from stderr to SLF4J
        }

        fragments.add(new Fragment(fragmentPath, evaluator));
      }  
      validateArguments();
    }
  
    @Override
    protected boolean doProcess2(Record inputRecord, InputStream stream) throws SaxonApiException, XMLStreamException {
      incrementNumRecords();      
      for (Fragment fragment : fragments) {
        Record template = inputRecord.copy();
        removeAttachments(template);
        XdmNode document = parseXmlDocument(stream);
        LOG.trace("XQuery input document: {}", document);
        XQueryEvaluator evaluator = fragment.xQueryEvaluator;
        evaluator.setContextItem(document);
        
        int i = 0;
        for (XdmItem item : evaluator) {
          i++;
          if (LOG.isTraceEnabled()) {
            LOG.trace("XQuery result sequence item #{} is of class: {} with value: {}", new Object[] { i,
                item.getUnderlyingValue().getClass().getName(), item });
          }
          if (item.isAtomicValue()) {
            LOG.debug("Ignoring atomic value in result sequence: {}", item);
            continue;
          }
          XdmNode node = (XdmNode) item;
          Record outputRecord = template.copy();
          boolean isNonEmpty = addRecordValues(node, Axis.SELF, XdmNodeKind.ATTRIBUTE, outputRecord);
          isNonEmpty = addRecordValues(node, Axis.ATTRIBUTE, XdmNodeKind.ATTRIBUTE, outputRecord) || isNonEmpty;
          isNonEmpty = addRecordValues(node, Axis.CHILD, XdmNodeKind.ELEMENT, outputRecord) || isNonEmpty;
          if (isNonEmpty) { // pass record to next command in chain   
            if (!getChild().process(outputRecord)) { 
              return false;
            }
          }
        }
      }      
      return true;
    }

    // extract fields from query result sequence
    private boolean addRecordValues(XdmNode node, Axis axis, XdmNodeKind nodeTest, Record record) {
      boolean isEmpty = true;
      XdmSequenceIterator iter = node.axisIterator(axis); 
      while (iter.hasNext()) {
        XdmNode child = (XdmNode) iter.next();
        if (child.getNodeKind() == nodeTest) { 
          String strValue = child.getStringValue();
          if (strValue.length() > 0) {
            record.put(child.getNodeName().getLocalName(), strValue);
            isEmpty = false;
          }
        }
      }
      return !isEmpty;
    }
    

    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////    
    private static final class Fragment {
      
      private final String fragmentPath;     
      private final XQueryEvaluator xQueryEvaluator;     
      
      public Fragment(String fragmentPath, XQueryEvaluator xQueryEvaluator) {
        this.fragmentPath = fragmentPath;
        this.xQueryEvaluator = xQueryEvaluator;
      }
    }
    
  }  
  
}
