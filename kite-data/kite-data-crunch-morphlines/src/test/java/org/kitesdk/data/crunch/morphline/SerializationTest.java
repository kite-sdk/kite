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
package org.kitesdk.data.crunch.morphline;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;


public class SerializationTest extends Assert {

  @Test
  public void testBasic() throws Exception {
    Foo foo = (Foo) roundTrip(new Foo());
    assertNotNull(foo.prng);
    assertNull(foo.trans);
  }

  private Object roundTrip(Object obj) throws Exception {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bout);
    out.writeObject(obj);
    out.close();
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()));
    return in.readObject();
  }

  private static final class Foo implements Serializable {
    
    private Foo() {}

    public final Random prng = new Random();
    public final transient Object trans = new Object();
    //public final Object obj = new Object();
  }
}
