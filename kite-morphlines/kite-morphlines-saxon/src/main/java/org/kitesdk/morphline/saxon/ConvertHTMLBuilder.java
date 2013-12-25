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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;

import org.ccil.cowan.tagsoup.HTMLSchema;
import org.ccil.cowan.tagsoup.Parser;
import org.ccil.cowan.tagsoup.XMLWriter;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;

import com.google.common.base.Charsets;
import com.typesafe.config.Config;

/**
 * Command that converts HTML to XHTML using the <a
 * href="http://ccil.org/~cowan/XML/tagsoup/">TagSoup</a> library.
 * 
 * Instead of parsing well-formed or valid XML, this command parses HTML as it is found in the wild:
 * poor, nasty and brutish, though quite often far from short. TagSoup (and hence this command) is
 * designed for people who have to process this stuff using some semblance of a rational application
 * design. By providing this converter, it allows standard XML tools to be applied to even the
 * worst HTML.
 */
public final class ConvertHTMLBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("convertHTML");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    try {
      return new ConvertHTML(this, config, parent, child, context);
    } catch (SAXNotRecognizedException e) {
      throw new MorphlineCompilationException("Cannot compile", config, e);
    } catch (SAXNotSupportedException e) {
      throw new MorphlineCompilationException("Cannot compile", config, e);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ConvertHTML extends AbstractParser {

    private final Charset charset;
    private final boolean omitXMLDeclaration;
    private final XMLReader xmlReader;
    private final HTMLSchema htmlSchema = new HTMLSchema();
  
    public ConvertHTML(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) throws SAXNotRecognizedException, SAXNotSupportedException {
      super(builder, config, parent, child, context);
      this.charset = getConfigs().getCharset(config, "charset", null);
      this.omitXMLDeclaration = getConfigs().getBoolean(config, "omitXMLDeclaration", false);      
      this.xmlReader = new Parser(); // no reuse?
      xmlReader.setProperty(Parser.schemaProperty, htmlSchema);
      xmlReader.setFeature(Parser.CDATAElementsFeature, getConfigs().getBoolean(config, "noCDATA", false));
      xmlReader.setFeature(Parser.namespacesFeature, !getConfigs().getBoolean(config, "noNamespaces", true));
      xmlReader.setFeature(Parser.ignoreBogonsFeature, getConfigs().getBoolean(config, "noBogons", false)); // also see TIKA-599
      xmlReader.setFeature(Parser.bogonsEmptyFeature, getConfigs().getBoolean(config, "emptyBogons", false));
      xmlReader.setFeature(Parser.rootBogonsFeature, getConfigs().getBoolean(config, "noRootBogons", false));
      xmlReader.setFeature(Parser.defaultAttributesFeature, getConfigs().getBoolean(config, "noDefaultAttributes", false));
      xmlReader.setFeature(Parser.translateColonsFeature, getConfigs().getBoolean(config, "noColons", false));
      xmlReader.setFeature(Parser.restartElementsFeature, getConfigs().getBoolean(config, "noRestart", false));
      xmlReader.setFeature(Parser.ignorableWhitespaceFeature, !getConfigs().getBoolean(config, "suppressIgnorableWhitespace", true));
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record inputRecord, InputStream stream) throws IOException {
      try {
        return doProcess2(inputRecord, stream);
      } catch (SAXNotRecognizedException e) {
        throw new MorphlineRuntimeException(e);
      } catch (SAXNotSupportedException e) {
        throw new MorphlineRuntimeException(e);
      } catch (SAXException e) {
        throw new MorphlineRuntimeException(e);
      }
    }
    
    private boolean doProcess2(Record inputRecord, InputStream stream) throws IOException, SAXException {      
      ByteArrayOutputStream out = new ByteArrayOutputStream(16 * 1024);
      XMLWriter xmlWriter = new XMLWriter(new BufferedWriter(new OutputStreamWriter(out, Charsets.UTF_8)));
      xmlWriter.setOutputProperty(XMLWriter.ENCODING, "UTF-8");
      if (omitXMLDeclaration) {
        xmlWriter.setOutputProperty(XMLWriter.OMIT_XML_DECLARATION, "yes");
      }
      xmlReader.setContentHandler(xmlWriter);
      Charset detectedCharset = detectCharset(inputRecord, charset);
      InputSource source = new InputSource(new BufferedReader(new InputStreamReader(stream, detectedCharset)));
      
      xmlReader.parse(source); // push the HTML through tagsoup into the output byte array
      
      Record outputRecord = inputRecord.copy();
      removeAttachments(outputRecord);
      outputRecord.replaceValues(Fields.ATTACHMENT_BODY, out.toByteArray());      
      incrementNumRecords();
        
      // pass record to next command in chain:
      if (!getChild().process(outputRecord)) {
        return false;
      }
      return true;        
    }
      
  }
  
}
