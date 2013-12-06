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
package org.kitesdk.morphline.saxon;

import java.io.InputStream;
import java.lang.ref.SoftReference;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Returns a StAX parser factory in preferred order. Some parsers are much more
 * reliable than others...
 * <p>
 * Caching the XMLInputFactory avoids expensive lookup and classpath
 * scanning.
 */
final class XMLInputFactoryCreator {
	
	private static final boolean CACHE_DTDS = true;
	
	private static final Logger LOG = LoggerFactory.getLogger(XMLInputFactoryCreator.class);

	/** Cached StAX XMLInputFactory; shared; thread safe */
	private SoftReference factoryRef = new SoftReference(null);
	
	public XMLInputFactoryCreator() {}
	
	/** Returns a (potentially cached) factory instance. */
	public synchronized XMLInputFactory getXMLInputFactory() {
		XMLInputFactory inputFactory = (XMLInputFactory) factoryRef.get();
		if (inputFactory == null) {
			inputFactory = createInputFactory();
			factoryRef = new SoftReference(inputFactory);
		}
		return inputFactory;
	}
	
	/**
	 * StAX parsers in preferred order. Some parsers are much more reliable than
	 * others...
	 */
	private static final String[] StAX_FACTORIES = {
		"com.ctc.wstx.stax.WstxInputFactory", // Woodstox (Codehaus, Apache license)
		"com.sun.xml.internal.stream.XMLInputFactoryImpl", // (Sun/Oracle)
//    "com.fasterxml.aalto.stax.InputFactoryImpl", // Aalto
//		"com.sun.xml.stream.ZephyrParserFactory", // sjsxp (Sun)
//		"oracle.xml.stream.OracleXMLInputFactory", // Oracle
//		"com.bea.xml.stream.MXParserFactory", // BEA
	};
	
//		private static void foo() {			
//		 I can never find or remember the values of those constants, 
//		 so here they are for future reference:
//		System.setProperty("javax.xml.stream.XMLInputFactory",  "com.ctc.wstx.stax.WstxInputFactory");
//		System.setProperty("javax.xml.stream.XMLOutputFactory", "com.ctc.wstx.stax.WstxOutputFactory");
//		System.setProperty("javax.xml.stream.XMLEventFactory",  "com.ctc.wstx.stax.evt.WstxEventFactory");
//		
//		System.setProperty("javax.xml.stream.XMLInputFactory",  "com.sun.xml.stream.ZephyrParserFactory");
//		System.setProperty("javax.xml.stream.XMLOutputFactory", "com.sun.xml.stream.ZephyrWriterFactory");
//		System.setProperty("javax.xml.stream.XMLEventFactory",  "com.sun.xml.stream.events.ZephyrEventFactory");
//		
//		System.setProperty("javax.xml.stream.XMLInputFactory",  "com.bea.xml.stream.MXParserFactory");
//	    System.setProperty("javax.xml.stream.XMLOutputFactory", "com.bea.xml.stream.XMLOutputFactoryBase");
//	    System.setProperty("javax.xml.stream.XMLEventFactory",  "com.bea.xml.stream.EventFactory");
//	}

	private XMLInputFactory createInputFactory() {
		XMLInputFactory factory;
		for (int i = 0; i < StAX_FACTORIES.length; i++) {
			try {
				factory = (XMLInputFactory) ClassLoaderUtil.newInstance(StAX_FACTORIES[i]);
				setupProperties(factory);				
				LOG.debug("Using XMLInputFactory {}", factory.getClass().getName());
				return factory;
			} catch (IllegalArgumentException e) {
				// keep on trying
			} catch (NoClassDefFoundError err) {
				// keep on trying
			} catch (Exception err) {
				// keep on trying
			}
		}
		
		try { // StAX default
			factory = XMLInputFactory.newInstance();
			setupProperties(factory);
		} catch (IllegalArgumentException ex) {
			throw new IllegalArgumentException(
					"Could not find or create a suitable StAX parser"
							+ " - check your classpath", ex);
		} catch (Exception ex) {
			throw new IllegalArgumentException(
					"Could not find or create a suitable StAX parser"
							+ " - check your classpath", ex);
		} catch (NoClassDefFoundError ex) {
			throw new IllegalArgumentException(
					"Could not find or create a suitable StAX parser"
							+ " - check your classpath", ex);
		}
		
		LOG.debug("Using default XMLInputFactory {}", factory.getClass().getName());
		return factory;
	}
	
	/** Initializes default parser properties, if any. */
	protected void setupProperties(XMLInputFactory factory) {		
		factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, Boolean.TRUE);
		factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);	
		factory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.TRUE);
		try {
		  factory.setProperty(XMLInputFactory.IS_VALIDATING, Boolean.FALSE);
    } catch (IllegalArgumentException e) {
      ; // we can live with that
    }
		try {
			factory.setProperty(
				XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.TRUE);
		} catch (IllegalArgumentException e) {
			; // we can live with that
		}

    factory.setXMLResolver(new XMLResolver() {
      @Override
      public InputStream resolveEntity(String publicID, String systemID, String baseURI, String namespace) {
        return new InputStream() {
          @Override
          public int read() { return -1; }
        };
      }
    });

		String factoryName = factory.getClass().getName();
		if (factoryName.equals("com.ctc.wstx.stax.WstxInputFactory")) { 
			try {
				// it's safer to disable woodstox lazy parsing, in particular with DTDs
				// see http://woodstox.codehaus.org/ConfiguringStreamReaders
				// see com.ctc.wstx.api.WstxInputProperties
				String P_LAZY_PARSING = "com.ctc.wstx.lazyParsing";
				factory.setProperty(P_LAZY_PARSING, Boolean.FALSE);
			} catch (IllegalArgumentException e) {
				; // shouldn't happen, but we can live with that
			}
			
			try {
				// enable/disable DTD caching (wstx default is to enable it)
				String P_CACHE_DTDS = "com.ctc.wstx.cacheDTDs";
				factory.setProperty(P_CACHE_DTDS, Boolean.valueOf(CACHE_DTDS));
			} catch (IllegalArgumentException e) {
				; // shouldn't happen, but we can live with that
			}
//		} else if (factory.isPropertySupported("report-cdata-event")) {}
		} else if (factoryName.equals("com.sun.xml.stream.ZephyrParserFactory")) {
			try {
				// workaround to tell sjsxp to not ignore CDATA events
				// see sjsxp-1_0/docs/ReleaseNotes.html
				String P_REPORT_CDATA = "report-cdata-event";
//				String P_REPORT_CDATA = "http://java.sun.com/xml/stream/properties/report-cdata-event";
				factory.setProperty(P_REPORT_CDATA, Boolean.TRUE);
			} catch (IllegalArgumentException e) {
				; // shouldn't happen, but we can live with that
			}
		}
	}
	
}
