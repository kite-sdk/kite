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
package org.kitesdk.morphline.useragent;

import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Record;

import ua_parser.Client;
import ua_parser.Parser;

public class UserAgentMorphlineTest extends AbstractMorphlineTest {

  @Test
  public void testRawAPI() throws Exception {
    String userAgentStr = "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3";
    Client client = new Parser().parse(userAgentStr);

    assertEquals("Mobile Safari", client.userAgent.family); 
    assertEquals("5", client.userAgent.major);
    assertEquals("1", client.userAgent.minor); 
    assertNull(client.userAgent.patch);

    assertEquals("iOS", client.os.family); 
    assertEquals("5", client.os.major); 
    assertEquals("1", client.os.minor);
    assertEquals("1", client.os.patch);
    assertNull(client.os.patchMinor);

    assertEquals("iPhone", client.device.family); 
  }  

  @Test
  public void testRawAPIWithUnknownUserAgent() throws Exception {
    String userAgentStr = "fp@$%3489jvp#3E";
    Client client = new Parser().parse(userAgentStr);
    
    assertNotNull(client);
    assertEquals("Other", client.userAgent.family); 
    assertNull(client.userAgent.major);
    assertNull(client.userAgent.minor); 
    assertNull(client.userAgent.patch);

    assertEquals("Other", client.os.family); 
    assertNull(client.os.major); 
    assertNull(client.os.minor);
    assertNull(client.os.patch);
    assertNull(client.os.patchMinor);

    assertEquals("Other", client.device.family); 
  }  

  @Test
  public void testBasic() throws Exception {
    testBasic("test-morphlines/userAgent");    
  }
  
  @Test
  public void testBasicWithDatabase() throws Exception {
    testBasic("test-morphlines/userAgentWithDatabase");    
  }

  private void testBasic(String morphlineFile) throws Exception {
    morphline = createMorphline(morphlineFile);    
      
    for (int i = 0; i < 3; i++) {
      Record record = new Record();
      String userAgentStr = "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3";
      record.put("user_agents", userAgentStr);
      Record expected = new Record();
      expected.put("user_agents", userAgentStr);
      expected.put("ua_family", "Mobile Safari");
      expected.put("device_family", "iPhone");
      expected.put("string_constant1", "foo");
      expected.put("string_constant2", "");
      expected.put("ua_family_and_version", "Mobile Safari/5.1");
      expected.put("os_family_and_version", "iOS5.1.1-foo@bar");
              
      processAndVerifySuccess(record, expected, true);
      
      record = new Record();
      record.put("user_agents", userAgentStr);
      processAndVerifySuccess(record, expected, true);
    }
  }
  
  private void processAndVerifySuccess(Record input, Record expected, boolean isSame) {
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(input));
    assertEquals(expected, collector.getFirstRecord());
    if (isSame) {
      assertSame(input, collector.getFirstRecord());    
    } else {
      assertNotSame(input, collector.getFirstRecord());    
    }
  }

}
