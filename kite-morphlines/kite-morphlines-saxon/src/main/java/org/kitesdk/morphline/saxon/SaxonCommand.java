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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;

import net.sf.saxon.s9api.BuildingStreamWriterImpl;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.ExtensionFunction;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmNode;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/** Base class for XQuery and XSLT */
abstract class SaxonCommand extends AbstractParser {
    
  protected final XMLInputFactory inputFactory = new XMLInputFactoryCreator().getXMLInputFactory();
  protected final DocumentBuilder documentBuilder;
  protected final Processor processor;
  protected final boolean isTracing;

  public SaxonCommand(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
    super(builder, config, parent, child, context);
    
    this.isTracing = getConfigs().getBoolean(config, "isTracing", false);
    boolean isLicensedSaxonEdition = getConfigs().getBoolean(config, "isLicensedSaxonEdition", false);
    this.processor = new Processor(isLicensedSaxonEdition);
    this.documentBuilder = processor.newDocumentBuilder();
    
    Config features = getConfigs().getConfig(config, "features", ConfigFactory.empty());
    for (Map.Entry<String, Object> entry : new Configs().getEntrySet(features)) {
      processor.setConfigurationProperty(entry.getKey(), entry.getValue());
    }
    
    for (String clazz : getConfigs().getStringList(config, "extensionFunctions", Collections.<String>emptyList())) {
      Object function;
      try {
        function = Class.forName(clazz).newInstance();
      } catch (Exception e) {
        throw new MorphlineCompilationException("Cannot instantiate extension function: " + clazz, config);
      }
      
      if (function instanceof ExtensionFunction) {
        processor.registerExtensionFunction((ExtensionFunction) function);              
//      }
//      else if (function instanceof ExtensionFunctionDefinition) {
//        processor.registerExtensionFunction((ExtensionFunctionDefinition) function);              
      } else {
        throw new MorphlineCompilationException("Extension function has wrong class: " + clazz, config);
      }
    }
  }

  @Override
  protected final boolean doProcess(Record inputRecord, InputStream stream) throws IOException {
    try {
      return doProcess2(inputRecord, stream);
    } catch (SaxonApiException e) {
      throw new MorphlineRuntimeException(e);
    } catch (XMLStreamException e) {
      throw new MorphlineRuntimeException(e);
    }
  }
  
  abstract protected boolean doProcess2(Record inputRecord, InputStream stream) throws IOException, SaxonApiException, XMLStreamException;
  
  protected XdmNode parseXmlDocument(File file) throws XMLStreamException, SaxonApiException, IOException {
    InputStream stream = new FileInputStream(file);
    try {
      if (file.getName().endsWith(".gz")) {
        stream = new GZIPInputStream(new BufferedInputStream(stream));
      }
      return parseXmlDocument(stream);
    } finally {
      stream.close();
    }
  }
  
  protected XdmNode parseXmlDocument(InputStream stream) throws XMLStreamException, SaxonApiException {
    XMLStreamReader reader = inputFactory.createXMLStreamReader(null, stream);
    BuildingStreamWriterImpl writer = documentBuilder.newBuildingStreamWriter();      
    new XMLStreamCopier(reader, writer).copy(false); // push XML into Saxon and build TinyTree
    reader.close();
    writer.close();
    XdmNode document = writer.getDocumentNode();
    return document;
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////    
  final class DefaultErrorListener implements ErrorListener {

    public void error(TransformerException e) throws TransformerException {
      LOG.error("Error: " + e.getMessageAndLocation(), e);
      throw e;
    }
    
    public void fatalError(TransformerException e) throws TransformerException {
      LOG.error("Fatal error: " + e.getMessageAndLocation(), e);
      throw e;
    }

    public void warning(TransformerException e) throws TransformerException {
      LOG.warn("Warning: " + e.getMessageAndLocation(), e);
    }
  }

}
