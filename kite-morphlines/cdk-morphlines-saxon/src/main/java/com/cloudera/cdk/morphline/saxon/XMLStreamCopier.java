// Copied and adapted from nux-1.6
/*
 * Copyright (c) 2005, The Regents of the University of California, through
 * Lawrence Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy). All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * (1) Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 
 * (2) Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * (3) Neither the name of the University of California, Lawrence Berkeley
 * National Laboratory, U.S. Dept. of Energy nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * You are under no obligation whatsoever to provide any bug fixes, patches, or
 * upgrades to the features, functionality or performance of the source code
 * ("Enhancements") to anyone; however, if you choose to make your Enhancements
 * available either publicly, or directly to Lawrence Berkeley National
 * Laboratory, without imposing a separate written license agreement for such
 * Enhancements, then you hereby grant the following license: a non-exclusive,
 * royalty-free perpetual license to install, use, modify, prepare derivative
 * works, incorporate into other computer software, distribute, and sublicense
 * such enhancements or derivative works thereof, in binary and source code
 * form.
 */
package com.cloudera.cdk.morphline.saxon;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * Reads all events from a given StAX {@link XMLStreamReader} parser and pipes
 * them into a given StAX {@link XMLStreamWriter} serializer.
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek $
 * @version $Revision: 1.3 $, $Date: 2006/12/16 01:58:19 $
 */
final class XMLStreamCopier {

	private final XMLStreamReader reader;
	private final XMLStreamWriter writer;
	
	public XMLStreamCopier(XMLStreamReader reader, XMLStreamWriter writer) {
		if (reader == null)
			throw new IllegalArgumentException("reader must not be null");
		if (writer == null)
			throw new IllegalArgumentException("writer must not be null");
		
		this.reader = reader;
		this.writer = writer;
	}
	
	/**
	 * Reads all events from the reader and pipes them into the writer.
	 * 
	 * @param isFragmentMode
	 *            if true, copies all events of the XML fragment starting at the
	 *            current cursor position, if false copies all events of the
	 *            document starting at the current cursor positions.
	 * @throws XMLStreamException
	 *             If an error occurs while copying the stream
	 */
	public void copy(boolean isFragmentMode) throws XMLStreamException {
		int ev = isFragmentMode ? 
			XMLStreamConstants.START_ELEMENT : XMLStreamConstants.START_DOCUMENT;
		reader.require(ev, null, null);

		int depth = 0;
		ev = reader.getEventType();
		while (true) {
			switch (ev) {
				case XMLStreamConstants.START_ELEMENT: {
					writer.writeStartElement(
					    nonNull(reader.getPrefix()), // fixup bug where woodstox-3.2.7 returns null
							reader.getLocalName(), 
							nonNull(reader.getNamespaceURI())); // Saxon requires nonNull
					copyAttributes();
					copyNamespaces();
					depth++;
					break;
				}
				case XMLStreamConstants.END_ELEMENT: {
					writer.writeEndElement();
					depth--;
					if (isFragmentMode && depth == 0) {
						writer.flush();
						return; // we're done
					}
					break;
				}
				case XMLStreamConstants.ATTRIBUTE: {
					// can happen as part of an XPath result sequence, or similar
					copyAttribute(0);
					break;
				}
				case XMLStreamConstants.START_DOCUMENT:	{
					copyStartDocument();
					break;
				}
				case XMLStreamConstants.END_DOCUMENT: {
					writer.writeEndDocument();
					writer.flush();
					return; // we're done
				}
				case XMLStreamConstants.PROCESSING_INSTRUCTION: {
					writer.writeProcessingInstruction(
							reader.getPITarget(), reader.getPIData());
					break;
				}
				case XMLStreamConstants.COMMENT: {
					writer.writeComment(reader.getText());
					break;
				}
				case XMLStreamConstants.CDATA: {
					writer.writeCData(reader.getText());
					break;
				}
				case XMLStreamConstants.SPACE:
				case XMLStreamConstants.CHARACTERS: {
					copyText();
					break;
				}
				case XMLStreamConstants.ENTITY_REFERENCE: {
//					writer.writeEntityRef(reader.getLocalName()); // don't expand the ref
					copyText(); // expand the ref (safer)
					break;
				}
				case XMLStreamConstants.DTD: {
					copyDTD();
					break;
				}
				case XMLStreamConstants.ENTITY_DECLARATION: 
					break; // ignore (handled by XMLStreamConstants.DTD)
				case XMLStreamConstants.NOTATION_DECLARATION: 
					break; // ignore (handled by XMLStreamConstants.DTD)
				case XMLStreamConstants.NAMESPACE: {
					// can happen as part of an XPath result sequence, or similar
					writer.writeNamespace(reader.getPrefix(), reader.getNamespaceURI());
					break;
				}
				default: {
					throw new XMLStreamException("Unrecognized event type: " 
							+ reader.getEventType());
				}
			}
			
			ev = reader.next();
		}
	}
	
	private void copyAttributes() throws XMLStreamException {
		int count = reader.getAttributeCount();
		for (int i = 0; i < count; i++) {
			copyAttribute(i);
		}
	}
	
	private void copyAttribute(int i) throws XMLStreamException {
		writer.writeAttribute(
				reader.getAttributePrefix(i),
				reader.getAttributeNamespace(i),
				reader.getAttributeLocalName(i),
				reader.getAttributeValue(i));
	}
	
	private void copyNamespaces() throws XMLStreamException {
		int count = reader.getNamespaceCount();
		for (int i = 0; i < count; i++) {
			String prefix = reader.getNamespacePrefix(i);
			String namespaceURI = nonNull(reader.getNamespaceURI(i)); // Saxon requires nonNull
			writer.writeNamespace(prefix, namespaceURI);
		}
	}

//	private char[] buf = new char[2048];
	private void copyText() throws XMLStreamException {
		writer.writeCharacters(reader.getText());
		
//		int len = buf.length;
//		for (int start = 0; ; start += len) {
//			int n = reader.getTextCharacters(start, buf, 0, len);
//			writer.writeCharacters(buf, 0, n);
//			if (n < len) break;
//		}
	}
	
	private void copyStartDocument() throws XMLStreamException {
		String encoding = reader.getEncoding();
		String version = reader.getVersion();
		if (version == null) version = "1.0";
		writer.writeStartDocument(encoding, version); 
	}	

	private void copyDTD() throws XMLStreamException {
		writer.writeDTD(reader.getText());
//		Nodes nodes = DTDParser.readDocType(reader, new NodeFactory());
//		if (nodes.size() > 0) {
//			String str = nodes.get(0).toXML();
//			writer.writeDTD(str);
//		}		
	}
	
	private String nonNull(String str) {
	  return str == null ? "" : str;
	}

}
