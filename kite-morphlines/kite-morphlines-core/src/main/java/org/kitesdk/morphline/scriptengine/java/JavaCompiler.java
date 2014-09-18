/*
 * Copyright (C) 2006 Sun Microsystems, Inc. All rights reserved. 
 * Use is subject to license terms.
 *
 * Redistribution and use in source and binary forms, with or without modification, are 
 * permitted provided that the following conditions are met: Redistributions of source code 
 * must retain the above copyright notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, this list of 
 * conditions and the following disclaimer in the documentation and/or other materials 
 * provided with the distribution. Neither the name of the Sun Microsystems nor the names of 
 * is contributors may be used to endorse or promote products derived from this software 
 * without specific prior written permission. 

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY 
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON 
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * JavaCompiler.java
 * @author A. Sundararajan
 */

package org.kitesdk.morphline.scriptengine.java;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

/**
 * Simple interface to Java compiler using JSR 199 Compiler API.
 */
class JavaCompiler {	
	private javax.tools.JavaCompiler tool;
	private StandardJavaFileManager stdManager;

	public JavaCompiler() {
		tool = ToolProvider.getSystemJavaCompiler();
		if (tool == null) {
			throw new RuntimeException("Could not get Java compiler. Please, ensure that JDK is used instead of JRE.");
		}
		stdManager = tool.getStandardFileManager(null, null, null);
	}

	public Map<String, byte[]> compile(String source, String fileName) {
		PrintWriter err = new PrintWriter(System.err);
		return compile(source, fileName, err, null, null);
	}

	public Map<String, byte[]> compile(String fileName, String source, 
									Writer err) {
		return compile(fileName, source, err, null, null);
	}

	public Map<String, byte[]> compile(String fileName, String source, 
									Writer err, String sourcePath) {
		return compile(fileName, source, err, sourcePath, null);
	}

	/**
	 * compile given String source and return bytecodes as a Map.
	 *
	 * @param fileName source fileName to be used for error messages etc.
	 * @param source Java source as String
	 * @param err error writer where diagnostic messages are written
	 * @param sourcePath location of additional .java source files
	 * @param classPath location of additional .class files
	 */
	public Map<String, byte[]> compile(String fileName, String source, 
					Writer err, String sourcePath, String classPath) {
		// to collect errors, warnings etc.
		DiagnosticCollector<JavaFileObject> diagnostics = 
			new DiagnosticCollector<JavaFileObject>();

		// create a new memory JavaFileManager
		MemoryJavaFileManager manager = new MemoryJavaFileManager(stdManager);

		// prepare the compilation unit
		List<JavaFileObject> compUnits = new ArrayList<JavaFileObject>(1);
		compUnits.add(manager.makeStringSource(fileName, source));

		// javac options
		List<String> options = new ArrayList<String>();
		options.add("-Xlint:all");
//		options.add("-g:none");
		options.add("-deprecation");
		if (sourcePath != null) {
			options.add("-sourcepath");
			options.add(sourcePath);
		}

		if (classPath != null) {
			options.add("-classpath");
			options.add(classPath);
		}
	   
		// create a compilation task
		javax.tools.JavaCompiler.CompilationTask task =
			tool.getTask(err, manager, diagnostics, 
						 options, null, compUnits);

		if (task.call() == false) {
			PrintWriter perr = new PrintWriter(err);
			for (Diagnostic diagnostic : diagnostics.getDiagnostics()) {				
				perr.println(diagnostic.getMessage(null));
			}
			perr.flush();
			return null;
		}

		Map<String, byte[]> classBytes = manager.getClassBytes();
		try {
			manager.close();
		} catch (IOException exp) {
		}

		return classBytes; 
	}
}
