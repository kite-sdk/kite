/* Modified from https://scripting.dev.java.net/" (JSR 223 Java Scripting) */

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
 * JavaScriptEngine.java
 * @author A. Sundararajan
 */

package org.kitesdk.morphline.scriptengine.java;

import java.io.Reader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.Map;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

/**
 * This is a fast script engine for Java programming language - I modified JSR
 * 223 Java Scripting a little for some 100x less overhead for invoking "static" methods.
 */
class FastJavaScriptEngine extends AbstractScriptEngine implements Compilable {

	// Java compiler
	private JavaCompiler compiler;

	public FastJavaScriptEngine() {
		compiler = new JavaCompiler();
	}

	// my implementation for CompiledScript
	public static final class JavaCompiledScript extends CompiledScript {
		
		private final Method targetMethod; // TODO: use java.lang.invoke.MethodHandle on Java7 for better perf; e.g. see http://vanillajava.blogspot.com/2011/08/methodhandle-performance-in-java-7.html

		@SuppressWarnings("unchecked")
		JavaCompiledScript (Class clazz, String methodName, Class[] parameterTypes) throws ScriptException {
			try {
				this.targetMethod = clazz.getMethod(methodName, parameterTypes);
			} catch (NoSuchMethodException e) {
				throw new ScriptException(e);
			}
			int modifiers = this.targetMethod.getModifiers();
			if (!Modifier.isPublic(modifiers) || !Modifier.isStatic(modifiers)) { 
				throw new ScriptException(
					"Cannot find public static method: " + methodName); }
		}

		public Object eval(Object... params) throws ScriptException {
			try {
				return targetMethod.invoke(null, params);
			} catch (Exception e) {
				throw new ScriptException(e);
			}
		}
		
		public ScriptEngine getEngine() {
			throw new UnsupportedOperationException();
		}

		public Object eval(ScriptContext ctx) throws ScriptException {
			throw new UnsupportedOperationException();
		}		

	}

	public CompiledScript compile(String script, String methodName, Class[] parameterTypes) throws ScriptException {
		Class clazz = parse(script, context); 
		return new JavaCompiledScript(clazz, methodName, parameterTypes);
	}

	public CompiledScript compile(String script) throws ScriptException {
		throw new UnsupportedOperationException();
	}

	public CompiledScript compile(Reader reader) throws ScriptException {		
		throw new UnsupportedOperationException();
	}

	public Object eval(String str, ScriptContext ctx) throws ScriptException {
		throw new UnsupportedOperationException();
	}

	public Object eval(Reader reader, ScriptContext ctx) throws ScriptException {
		throw new UnsupportedOperationException();
	}

	public ScriptEngineFactory getFactory() {
		throw new UnsupportedOperationException();
	}

	public Bindings createBindings() {
		return new SimpleBindings();
	}


	// Internals only below this point

	private Class parse(String str, ScriptContext ctx) throws ScriptException {		
		String fileName = getFileName(ctx);
		String sourcePath = getSourcePath(ctx);
		String classPath = getClassPath(ctx);

		Map<String, byte[]> classBytes = compiler.compile(fileName, str,
							ctx.getErrorWriter(), sourcePath, classPath);

		if (classBytes == null) {
			throw new ScriptException("compilation failed");
		}

		// create a ClassLoader to load classes from MemoryJavaFileManager
		MemoryClassLoader loader = new MemoryClassLoader(classBytes, classPath,
											getParentLoader(ctx));

		String mainClassName = getMainClassName(ctx);
		if (mainClassName != null) {
			try {
				Class clazz = loader.load(mainClassName);
				Method mainMethod = findMainMethod(clazz);
				if (mainMethod == null) {
					throw new ScriptException("no main method in " + mainClassName);
				}
				return clazz;
			} catch (ClassNotFoundException cnfe) {
				throw new ScriptException(cnfe);
			}
		}

		// no main class configured - load all compiled classes
		Iterable<Class> classes;
		try {
			classes = loader.loadAll();
		} catch (ClassNotFoundException exp) {
			throw new ScriptException(exp);
		}
		
		// search for class with main method
		Class c = findMainClass(classes);
		if (c != null) {
			return c;
		} else {
			// if class with "main" method, then
			// return first class
			Iterator<Class> itr = classes.iterator();
			if (itr.hasNext()) {
				return itr.next();
			} else {
				return null;
			}
		} 
	}
	
	private static Class findMainClass(Iterable<Class> classes) {
		// find a public class with public static main method
		for (Class clazz : classes) {
			int modifiers = clazz.getModifiers();
			if (Modifier.isPublic(modifiers)) {
				Method mainMethod = findMainMethod(clazz);
				if (mainMethod != null) {
					return clazz;
				}				
			}
		}

		// okay, try to find package private class that
		// has public static main method
		for (Class clazz : classes) {
			Method mainMethod = findMainMethod(clazz);
			if (mainMethod != null) {
				return clazz;
			}
		}

		// no main class found!
		return null;
	}

	// find public static void main(String[]) method, if any
  @SuppressWarnings("unchecked")
	private static Method findMainMethod(Class clazz) {
		try {
			Method mainMethod = clazz.getMethod("main", new Class[] { String[].class });
			int modifiers = mainMethod.getModifiers();
			if (Modifier.isPublic(modifiers) && 
				Modifier.isStatic(modifiers)) {
				return mainMethod;
			}
		} catch (NoSuchMethodException nsme) {
		}
		return null;
	}

	private static String getFileName(ScriptContext ctx) {
		int scope = ctx.getAttributesScope(ScriptEngine.FILENAME);
		if (scope != -1) {
			return ctx.getAttribute(ScriptEngine.FILENAME, scope).toString();
		} else {
			return "$unnamed.java";
		}
	}


	// for certain variables, we look for System properties. This is
	// the prefix used for such System properties
	private static final String SYSPROP_PREFIX = "com.sun.script.java.";

	private static final String SOURCEPATH = "sourcepath";
	private static String getSourcePath(ScriptContext ctx) {
		int scope = ctx.getAttributesScope(SOURCEPATH);
		if (scope != -1) {
			return ctx.getAttribute(SOURCEPATH).toString();
		} else {
			// look for "com.sun.script.java.sourcepath"
			return System.getProperty(SYSPROP_PREFIX + SOURCEPATH);
		}		
	}

	private static final String CLASSPATH = "classpath";
	private static String getClassPath(ScriptContext ctx) {
		int scope = ctx.getAttributesScope(CLASSPATH);
		if (scope != -1) {
			return ctx.getAttribute(CLASSPATH).toString();
		} else {
			// look for "com.sun.script.java.classpath"
			String res = System.getProperty(SYSPROP_PREFIX + CLASSPATH);
			if (res == null) {
				res = System.getProperty("java.class.path");
			}
			return res;
		}
	}

	private static final String MAINCLASS = "mainClass";
	private static String getMainClassName(ScriptContext ctx) {
		int scope = ctx.getAttributesScope(MAINCLASS);
		if (scope != -1) {
			return ctx.getAttribute(MAINCLASS).toString();
		} else {
			// look for "com.sun.script.java.mainClass"
			return System.getProperty(SYSPROP_PREFIX + MAINCLASS);
		}		
	}

	private static final String PARENTLOADER = "parentLoader";
	private static ClassLoader getParentLoader(ScriptContext ctx) {
		int scope = ctx.getAttributesScope(PARENTLOADER);
		if (scope != -1) {
			Object loader = ctx.getAttribute(PARENTLOADER);
			if (loader instanceof ClassLoader) {
				return (ClassLoader) loader;
			} // else fall through..
		}
		return null;	   
	}

}
