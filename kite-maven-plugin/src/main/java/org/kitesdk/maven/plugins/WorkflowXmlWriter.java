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
package org.kitesdk.maven.plugins;

import com.google.common.base.Joiner;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.WriterFactory;
import org.codehaus.plexus.util.xml.PrettyPrintXMLWriter;
import org.codehaus.plexus.util.xml.XMLWriter;

class WorkflowXmlWriter {

  private final String encoding;

  private static final String WORKFLOW_ELEMENT = "workflow-app";

  WorkflowXmlWriter(String encoding) {
    this.encoding = encoding;
  }

  protected Writer initializeWriter(final File destinationFile) throws
      MojoExecutionException {
    try {
      return WriterFactory.newXmlWriter(destinationFile);
    } catch (IOException e) {
      throw new MojoExecutionException("Exception while opening file[" +
          destinationFile.getAbsolutePath() + "]", e);
    }
  }

  protected XMLWriter initializeXmlWriter(final Writer writer, final String docType) {
    return new PrettyPrintXMLWriter(writer, encoding, docType);
  }

  protected void write(Workflow workflow) throws MojoExecutionException {
    Writer w = initializeWriter(workflow.getDestinationFile());
    XMLWriter writer = initializeRootElement(w, workflow.getSchemaVersion(),
        workflow.getName());

    appendStart(writer);
    appendJavaAction(writer, workflow);
    appendKill(writer);
    appendEnd(writer);

    writer.endElement();
    close(w);
  }

  private XMLWriter initializeRootElement(Writer w, String schemaVersion,
      String workflowName) {
    XMLWriter writer = initializeXmlWriter(w, null);
    writer.startElement(WORKFLOW_ELEMENT);
    writer.addAttribute("xmlns", "uri:oozie:workflow:" + schemaVersion);
    writer.addAttribute("name", workflowName);
    return writer;
  }

  private void appendStart(XMLWriter writer) {
    writer.startElement("start");
    writer.addAttribute("to", "java-node");
    writer.endElement();
  }

  private void appendJavaAction(XMLWriter writer, Workflow workflow) throws
      MojoExecutionException {
    Properties hadoopConfiguration = workflow.getHadoopConfiguration();

    writer.startElement("action");
    writer.addAttribute("name", "java-node");

    writer.startElement("java");
    doWriteElement(writer, "job-tracker", "${" + AbstractAppMojo.JOBTRACKER_PROPERTY + "}");
    doWriteElement(writer, "name-node", "${" + AbstractAppMojo.NAMENODE_PROPERTY + "}");
    doWriteElement(writer, "main-class", workflow.getToolClass());
    for (String key : hadoopConfiguration.stringPropertyNames()) {
      String value = hadoopConfiguration.getProperty(key);
      doWriteElement(writer, "arg", "-D");
      doWriteElement(writer, "arg", key + "=" + value);
    }
    if (!workflow.getLibJars().isEmpty()) {
      doWriteElement(writer, "arg", "-libjars");
      doWriteElement(writer, "arg", Joiner.on(',').join(workflow.getLibJars()));
    }
    String[] args = workflow.getArgs();
    if (args != null) {
      for (String arg : args) {
        doWriteElement(writer, "arg", arg);
      }
    }
    writer.endElement();

    doWriteElement(writer, "ok", "to", "end");
    doWriteElement(writer, "error", "to", "fail");

    writer.endElement();
  }

  private void appendKill(XMLWriter writer) {
    writer.startElement("kill");
    writer.addAttribute("name", "fail");
    doWriteElement(writer, "message", "Java failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");
    writer.endElement();
  }

  private void appendEnd(XMLWriter writer) {
    writer.startElement("end");
    writer.addAttribute("name", "end");
    writer.endElement();
  }

  private void doWriteElement(XMLWriter writer, String element, String attributeName,
      String attributeValue) {
    writer.startElement(element);
    writer.addAttribute(attributeName, attributeValue);
    writer.endElement();
  }

  private void doWriteElement(XMLWriter writer, String element, String text) {
    writer.startElement(element);
    writer.writeText(text);
    writer.endElement();
  }

  protected void close(Writer closeable) {
    IOUtils.closeQuietly(closeable);
  }
}
