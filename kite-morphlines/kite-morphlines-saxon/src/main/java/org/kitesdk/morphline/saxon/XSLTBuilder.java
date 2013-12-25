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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.s9api.QName;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;
import net.sf.saxon.s9api.XsltTransformer;
import net.sf.saxon.stax.XMLStreamWriterDestination;
import net.sf.saxon.trace.XQueryTraceListener;
import net.sf.saxon.value.UntypedAtomicValue;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Configs;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Command that parses an InputStream that contains an XML document and runs the given XSL Transform
 * over the XML document. For each item in the query result sequence, the command emits a morphline
 * record containing the item's name-value pairs.
 * 
 * TODO: Add support for streaming via fragmentPath.
 */
public final class XSLTBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("xslt");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    try {
      return new XSLT(this, config, parent, child, context);
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
  private static final class XSLT extends SaxonCommand {
    
    private final List<Fragment> fragments = new ArrayList();
  
    public XSLT(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) throws SaxonApiException, IOException, XMLStreamException {
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
        
        XsltCompiler compiler = processor.newXsltCompiler();
        compiler.setErrorListener(new DefaultErrorListener());
        compiler.setCompileWithTracing(isTracing);
        String version = getConfigs().getString(config, "languageVersion", null);
        if (version != null) {
          compiler.setXsltLanguageVersion(version);
        }
        
        XsltExecutable executable = null;
        String query = getConfigs().getString(fragment, "queryString", null);
        if (query != null) {
          executable = compiler.compile(new StreamSource(new StringReader(query)));     
        }
        String queryFile = getConfigs().getString(fragment, "queryFile", null);
        if (queryFile != null) {
          executable = compiler.compile(new StreamSource(new File(queryFile)));     
        }
        if (query == null && queryFile == null) {
          throw new MorphlineCompilationException("Either query or queryFile must be defined", config);
        }
        if (query != null && queryFile != null) {
          throw new MorphlineCompilationException("Must not define both query and queryFile at the same time", config);
        }
        
        XsltTransformer evaluator = executable.load();
        Config variables = getConfigs().getConfig(fragment, "parameters", ConfigFactory.empty());
        for (Map.Entry<String, Object> entry : new Configs().getEntrySet(variables)) {
          XdmValue xdmValue = XdmNode.wrap(new UntypedAtomicValue(entry.getValue().toString()));
          evaluator.setParameter(new QName(entry.getKey()), xdmValue);
        }
        Config fileVariables = getConfigs().getConfig(fragment, "fileParameters", ConfigFactory.empty());
        for (Map.Entry<String, Object> entry : new Configs().getEntrySet(fileVariables)) {
          File file = new File(entry.getValue().toString());
          XdmValue doc = parseXmlDocument(file);
          evaluator.setParameter(new QName(entry.getKey()), doc);
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
        Record outputRecord = inputRecord.copy();
        removeAttachments(outputRecord);   
        XdmNode document = parseXmlDocument(stream);
        LOG.trace("XSLT input document: {}", document);
        XsltTransformer evaluator = fragment.transformer;
        evaluator.setInitialContextNode(document);
        XMLStreamWriter morphlineWriter = new MorphlineXMLStreamWriter(getChild(), outputRecord);
        evaluator.setDestination(new XMLStreamWriterDestination(morphlineWriter));
        evaluator.transform(); //  run the query and push into child via RecordXMLStreamWriter
      }      
      return true;
    }


    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////    
    private static final class Fragment {
      
      private final String fragmentPath;     
      private final XsltTransformer transformer;     
      
      public Fragment(String fragmentPath, XsltTransformer transformer) {
        this.fragmentPath = fragmentPath;
        this.transformer = transformer;
      }
    }
    
    
    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////    
    private static final class MorphlineXMLStreamWriter implements XMLStreamWriter {
      
      private final Command child;      
      private final Record template;
      private Record record;

      private int depth = 0;
      private String fieldName;
      private final StringBuilder stringValue = new StringBuilder();
      private boolean isEmpty = true;
      
      private NamespaceContext rootContext;
      private final Map properties = null; // new HashMap();      

      public MorphlineXMLStreamWriter(Command child, Record template) {
        this.child = child;
        this.template = template;
        this.record = template.copy();
      }
      
      @Override
      public void close() throws XMLStreamException {
      }
      
      @Override
      public void writeStartElement(String prefix, String localName,
          String namespaceURI) throws XMLStreamException {

        Preconditions.checkNotNull(localName);
        if (depth == 1) {
          fieldName = localName;
        }
        depth++;
      }

      @Override
      public void writeAttribute(String prefix, String namespaceURI,
          String localName, String value) throws XMLStreamException {
        
        if (depth <= 1) {
          if (value == null) {
            value = "";
          }
          if (value.length() > 0) {
            put(localName, value);
          }
        }
      }

       @Override
      public void writeEndElement() throws XMLStreamException {
        if (inPrologOrEpilog()) {
          throw new XMLStreamException(
            "Imbalanced element tags; attempted to write an end tag for a nonexistent start tag");
        }
        depth--;
        if (depth == 1) {
          Preconditions.checkNotNull(fieldName);
          if (stringValue.length() > 0) {
            put(fieldName, stringValue.toString());
            stringValue.setLength(0);
          }
          fieldName = null;
        }
        if (inPrologOrEpilog()) {
          if (!isEmpty) {
            child.process(record); // TODO throw exception if child returns false?
          }
          isEmpty = true;
          record = template.copy();
        }
      }

      private void put(String key, String value) {
        record.put(key, value);
        isEmpty = false;
      }
      
      @Override
      public void writeCharacters(String text) throws XMLStreamException {
        if (depth > 1) {
          stringValue.append(text);
        }
      }
      
      private boolean inPrologOrEpilog() {
        return depth == 0;
      }

      @Override
      public Object getProperty(String name) throws IllegalArgumentException {
        if (name == null)
          throw new IllegalArgumentException("Property name must not be null");

        if (!properties.containsKey(name))
          throw new IllegalArgumentException("Unsupported property: " + name);

        return properties.get(name);
      }

      @Override
      public NamespaceContext getNamespaceContext() {
        return rootContext;
      }

      @Override
      public void setNamespaceContext(NamespaceContext rootContext) throws XMLStreamException {
        this.rootContext = rootContext;
      }

      @Override
      public void setDefaultNamespace(String uri) throws XMLStreamException {   
      }

      @Override
      public void setPrefix(String prefix, String uri) throws XMLStreamException {    
      }
      
      @Override
      public String getPrefix(String uri) throws XMLStreamException {
        throw new UnsupportedOperationException("unreachable");
      }
        
      @Override
      public void writeStartDocument() throws XMLStreamException {
        writeStartDocument("1.0");
      }

      @Override
      public void writeStartDocument(String version) throws XMLStreamException {
        writeStartDocument("UTF-8", version);
      }

      @Override
      public void writeStartDocument(String encoding, String version)
          throws XMLStreamException {
      }

      @Override
      public void writeStartElement(String localName) throws XMLStreamException {
        String defaultNamespaceURI = "";
        String prefix = "";
        writeStartElement(prefix, localName, defaultNamespaceURI);
      }

      @Override
      public void writeStartElement(String namespaceURI, String localName)
          throws XMLStreamException {

        String prefix = "";
        writeStartElement(prefix, localName, namespaceURI);
      }

      @Override
      public void writeDefaultNamespace(String namespaceURI) throws XMLStreamException {
        writeNamespace("", namespaceURI);
      }

      @Override
      public void writeNamespace(String prefix, String namespaceURI)
          throws XMLStreamException {

      }
      
      @Override
      public void writeAttribute(String localName, String value) throws XMLStreamException {
        writeAttribute("", "", localName, value); // Attributes don't inherit default namespace
      }

      @Override
      public void writeAttribute(String namespaceURI, String localName, String value) 
          throws XMLStreamException {
        
        String prefix = "";
        writeAttribute(prefix, namespaceURI, localName, value);
      }

      @Override
      public void writeEndDocument() throws XMLStreamException {
      }

      @Override
      public void flush() throws XMLStreamException {
      }

      @Override
      public void writeCharacters(char[] text, int start, int len) throws XMLStreamException {
        writeCharacters(new String(text, start, len));
      }

      @Override
      public void writeCData(String data) throws XMLStreamException {
        writeCharacters(data);
      }

      @Override
      public void writeEntityRef(String name) throws XMLStreamException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void writeDTD(String dtd) throws XMLStreamException {
      }

      @Override
      public void writeProcessingInstruction(String target) throws XMLStreamException {
        writeProcessingInstruction(target, "");
      }

      @Override
      public void writeProcessingInstruction(String target, String data) throws XMLStreamException {        
      }

      @Override
      public void writeComment(String data) throws XMLStreamException {
      }

      @Override
      public void writeEmptyElement(String localName) throws XMLStreamException {
        writeStartElement(localName);
        writeEndElement();
      }

      @Override
      public void writeEmptyElement(String namespaceURI, String localName)
          throws XMLStreamException {
        
        writeStartElement(namespaceURI, localName);
        writeEndElement();
      }

      @Override
      public void writeEmptyElement(String prefix, String localName,
          String namespaceURI) throws XMLStreamException {
        
        writeStartElement(prefix, localName, namespaceURI);
        writeEndElement();
      }

    }
  }
}
