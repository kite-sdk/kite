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
package org.kitesdk.morphline.scriptengine.java;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Creates and compiles the given Java code block, wrapped into a Java method with the given return
 * type and parameter types, along with a Java class definition that contains the given import
 * statements.
 * <p>
 * Compilation is done in main memory, i.e. without writing to the filesystem.
 * <p>
 * The result is an object that can be executed (and reused) any number of times. This is a high
 * performance implementation, using an optimized variant of https://scripting.dev.java.net/" (JSR
 * 223 Java Scripting). Calling {@link #evaluate(Object...)} just means calling
 * {@link Method#invoke(Object, Object...)} and as such has the same minimal runtime cost, i.e.
 * O(100M calls/sec/core).
 * 
 * Instances of this class are thread-safe if the user provided script statements are thread-safe.
 */
public class ScriptEvaluator<T> {

  private final FastJavaScriptEngine.JavaCompiledScript compiledScript;
  private final String javaCodeBlock;
  private final String parseLocation;
  
  private static final AtomicLong nextClassNum = new AtomicLong();
  
  private static final String METHOD_NAME = "eval";

  private static final Logger LOG = LoggerFactory.getLogger(ScriptEvaluator.class);
  
  public ScriptEvaluator(String javaImports, String javaCodeBlock, Class<T> returnType,
      String[] parameterNames, Class[] parameterTypes,
      String parseLocation) throws ScriptException {
    this(javaImports, javaCodeBlock, returnType, parameterNames, parameterTypes, new Class[0], parseLocation);
  }
  
  public ScriptEvaluator(String javaImports, String javaCodeBlock, Class<T> returnType, 
      String[] parameterNames, Class[] parameterTypes, Class[] throwTypes,
      String parseLocation) throws ScriptException {
    
    if (parameterNames.length != parameterTypes.length) { 
      throw new IllegalArgumentException(
        "Lengths of parameterNames (" + parameterNames.length
            + ") and parameterTypes (" + parameterTypes.length
            + ") do not match"); 
    }
    
    this.javaCodeBlock = javaCodeBlock;
    this.parseLocation = parseLocation;    
    String myPackageName = getClass().getName();
    myPackageName = myPackageName.substring(0, myPackageName.lastIndexOf('.'));
    String className = "MyJavaClass" + nextClassNum.incrementAndGet();
    String returnTypeName = (returnType == Void.class ? "void" : returnType.getCanonicalName());
    
    String script = 
      "package " + myPackageName + ".scripts;"
      + "\n"
      + javaImports
      + "\n"
      + "\n public final class " + className + " {"    
      + "\n   public static " + returnTypeName + " " + METHOD_NAME + "(";
    
    for (int i = 0; i < parameterNames.length; i++) {
      if (i > 0) {
        script += ", ";
      }
      script += parameterTypes[i].getCanonicalName() + " " + parameterNames[i];
    }
    script += ") ";
    
    if (throwTypes.length > 0) {
      script += "throws ";
      for (int i = 0; i < throwTypes.length; i++) {
        if (i > 0) {
          script += ", ";
        }
        script += throwTypes[i].getCanonicalName();
      }
      script += " ";
    }
    
    script += "{ " + javaCodeBlock + " }";     
    script += "\n }";
    LOG.trace("Compiling script: \n{}", script);    
    
    FastJavaScriptEngine engine = new FastJavaScriptEngine();
    StringWriter errorWriter = new StringWriter();
    engine.getContext().setErrorWriter(errorWriter);
    engine.getContext().setAttribute(ScriptEngine.FILENAME, className + ".java", ScriptContext.ENGINE_SCOPE);
    ClassLoader[] loaders = getClassLoaders();
    engine.getContext().setAttribute("parentLoader", loaders[0], ScriptContext.ENGINE_SCOPE);
    try {
      compiledScript = (FastJavaScriptEngine.JavaCompiledScript) engine.compile(script, METHOD_NAME, parameterTypes);
    } catch (ScriptException e) {
      String errorMsg = errorWriter.toString();
      if (errorMsg.length() > 0) {
        errorMsg = ": " + errorMsg;
      }
      throwScriptCompilationException(parseLocation, e.getMessage() + errorMsg, null);
      throw null; // keep compiler happy
    }    
    engine.getContext().setErrorWriter(new PrintWriter(System.err, true)); // reset
  }
  
  @SuppressWarnings("unchecked")
  public T evaluate(Object... params) throws ScriptException {
    // TODO: consider restricting permissions/sandboxing; also see http://worldwizards.blogspot.com/2009/08/java-scripting-api-sandbox.html
    try {
      return (T) compiledScript.eval(params);
    } catch (ScriptException e) {        
      throwScriptExecutionException(parseLocation + " near: '" + javaCodeBlock + "'", params, e);
    }
    return null; // keep compiler happy
  }

  private ClassLoader[] getClassLoaders() {
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader myLoader = getClass().getClassLoader();
    if (contextLoader == null) {
      return new ClassLoader[] { myLoader };
    } else if (contextLoader == myLoader || myLoader == null) {
      return new ClassLoader[] { contextLoader };
    } else {
      return new ClassLoader[] { contextLoader, myLoader };
    }
  }
  
  private static void throwScriptCompilationException(String parseLocation, String msg, Throwable t) 
      throws ScriptException {
    
    if (t == null) {
      throw new ScriptException("Cannot compile script: " + parseLocation + " caused by " + msg);
    } else {
      ScriptException se = new ScriptException("Cannot compile script: " + parseLocation + " caused by " + msg);
      se.initCause(t);
      throw se;
    }
  }
  
  private static void throwScriptExecutionException(String parseLocation, Object[] params, Throwable e) 
      throws ScriptException {
    
    ScriptException se = new ScriptException("Cannot execute script: " + parseLocation + " for params " + Arrays.asList(params).toString());
    se.initCause(e);
    throw se;
  }
  
}
