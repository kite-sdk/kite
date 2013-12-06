// Copied from nux-1.6
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to instantiate objects via reflection in a safe way,
 * even in weird multi class loader environments.
 * <p>
 * This does basically the same as the SAX <code>NewInstance</code> class used by
 * <code>XMLReaderFactory.createXMLReader(String className)</code>.
 * <p>
 * Also see javax.xml.parsers.FactoryFinder.newInstance(...)
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek $
 * @version $Revision: 1.1 $, $Date: 2006/12/07 04:56:46 $
 */
final class ClassLoaderUtil {

	// if there is a weird ClassLoader related problem, try switching this flag 
	// and see if that helps
	private static final boolean SIMPLE_MODE = false; 
	
	private static final boolean FALL_BACK = true;

	private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderUtil.class);
	
	private ClassLoaderUtil() {} // not instantiable

	public static Object newInstance(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		if (SIMPLE_MODE) return Class.forName(className).newInstance();

		// instantiate a temporary dummy object and get its context class loader
		ClassLoader classLoader = new ClassLoaderFinder().getContextClassLoader();
		
		Class clazz;
		if (classLoader == null) {
			LOG.debug("No context class loader found");
			clazz = Class.forName(className);
		} else {
		  LOG.debug("Context class loader found");
			try {
				clazz = classLoader.loadClass(className);
			} catch (ClassNotFoundException ex) {
				if (FALL_BACK) { 
					// Fall back to current classloader (e.g. interactive eclipse invocation)
				  LOG.debug("Fall back to current classloader");
					classLoader = ClassLoaderUtil.class.getClassLoader();
					clazz = classLoader.loadClass(className);
				} else {
					throw ex;
				}
			}
		}
		return clazz.newInstance();
	}

	private static final class ClassLoaderFinder {
		public ClassLoader getContextClassLoader() {
			return Thread.currentThread().getContextClassLoader();
		}
	}

}
