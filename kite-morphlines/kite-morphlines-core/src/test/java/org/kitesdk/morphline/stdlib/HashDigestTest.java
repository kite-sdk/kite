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
package org.kitesdk.morphline.stdlib;

import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

public class HashDigestTest extends AbstractMorphlineTest {

  @Test
  public void testHashDigest() throws Exception {
    morphline = createMorphline("test-morphlines/hashDigest");
    Record record = new Record();
    String if1 = "The quick brown fox jumped over the lazy dog";
    String if2 = "Lorem ipsum dolor sit amet, cu vel vero liber vivendum, liber labitur senserit ei cum. Qui ex soluta viderer, inimicus pertinacia assueverit ad mei, ut esse melius dissentias ius. Ei nec delicata splendide dissentiet, dicit ridens feugiat an qui. Ut quot diceret usu. Qui affert commodo voluptatibus ne, quis euismod instructior ne ius.";
    String if3 = "Hello world!";
    
    
    record.put("if1", if1);
    record.put("if2", if2);
    
    record.put("of2","don'tdeleteme");
    
    record.put("if3", if3);
    record.put("of3", "deleteme");
    
    byte[] bytes = if1.getBytes("UTF-8");
    record.put(Fields.ATTACHMENT_BODY, bytes);
    
    
    Record expected = new Record();
    expected.put("if1", if1);
    expected.put("if2", if2);
    expected.put("if3", if3);
    expected.put("of2","don'tdeleteme");


    expected.put("of1", "08A008A01D498C404B0C30852B39D3B8".toLowerCase());
    expected.put("of2", "B80EBD810BF0287790CD107DB2C3FF21E6A2BA5C".toLowerCase());
    expected.put("of3", "C0535E4BE2B79FFD93291305436BF889314E4A3FAEC05ECFFCBB7DF31AD9E51A".toLowerCase());
    expected.put("of4", "08A008A01D498C404B0C30852B39D3B8".toLowerCase());
    expected.put("of5", "108e2d013753189cb812c41e99e565f5".toLowerCase());
    
    processAndVerifySuccess(record, expected);
  }
  
  

  private void processAndVerifySuccess(Record input, Record expected) {
    processAndVerifySuccess(input, expected, true);
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
