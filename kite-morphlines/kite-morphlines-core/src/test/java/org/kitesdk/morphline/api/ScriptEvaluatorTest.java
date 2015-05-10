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
package org.kitesdk.morphline.api;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.morphline.scriptengine.java.ScriptEvaluator;

import com.google.common.collect.ArrayListMultimap;


@SuppressWarnings("unchecked")
public class ScriptEvaluatorTest extends Assert {

  private String javaImports;
  
  @Before
  public void setUp() {
    javaImports = ""
      + "import java.util.regex.*;\n" 
      + "import java.util.*;" 
    ;
  }
  
	@Test
	public void testBasic() throws Exception {
		ScriptEvaluator script = new ScriptEvaluator(javaImports, "return x * 2; ", Integer.class, new String[] { "x" }, new Class[] { Integer.class }, new Class[] {RuntimeException.class, IOException.class}, "myQuery");
		Object result = script.evaluate(new Object[] { new Integer(1) });
		assertEquals(result, new Integer(2));
	}

	@Test
	public void testVoid() throws Exception {
		ScriptEvaluator script = new ScriptEvaluator(javaImports, "int foo = 0; ", Void.class, new String[] { "x" }, new Class[] { Integer.class }, "myQuery");
		Object result = script.evaluate(new Object[] { new Integer(1) });
		assertNull(result);
	}

	@Test
	public void testArray() throws Exception {
		ScriptEvaluator script = new ScriptEvaluator(javaImports, "return x; ", Integer[].class, new String[] { "x" }, new Class[] { Integer[].class }, "myQuery");
		Object result = script.evaluate(new Object[] { new Integer[] { new Integer(1) }});
		assertEquals(((Integer[]) result)[0], new Integer(1));
	}

	@Test
	public void testExternalObject() throws Exception {
		ArrayListMultimap.create();
    ScriptEvaluator script = new ScriptEvaluator(
        javaImports,
        "com.google.common.collect.ArrayListMultimap.create(); new org.kitesdk.morphline.api.Record(); return x.copy(); ",
        Record.class, new String[] { "x" }, new Class[] { Record.class }, "myQuery");
		Object result = script.evaluate(new Object[] { new Record() });
    assertEquals(result, new Record());
    assertTrue(result != new Record());
	}

	@Test
	public void testBenchmark() throws Exception {
//		long runs = 3000000000L;
//		long runs = 1000000000;
		long runs = 1000000;
		ScriptEvaluator script = new ScriptEvaluator(javaImports, "return x; ", Integer.class, new String[] { "x" }, new Class[] { Integer.class }, "myQuery");
		long start = System.currentTimeMillis();
		int checksum = 0;
		for (long i = 0; i < runs; i++) {
			Object result = script.evaluate(new Integer(1));
//			Integer result = (Integer) script.evaluate(new Integer(1));
//			Integer result = new Long(i).intValue();
			checksum += (result != null ? 1 : 0);
//			checksum += result;
		}
		float secs = (System.currentTimeMillis() - start) / 1000.0f;
		System.out.println("Took secs = " + secs + ", iters/sec = " + (runs / secs));
		System.out.println(checksum);
	}

}
