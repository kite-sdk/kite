/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.spi;

import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.ColumnMappingDescriptor;

import static org.junit.Assert.assertEquals;

public class TestColumnMappingDescriptorParser {
  
  private ColumnMappingDescriptorParser parser;
  
  @Before
  public void before() throws IOException {
    parser = new ColumnMappingDescriptorParser();
  }
  
  @Test
  public void testBasic() {
    ColumnMappingDescriptor desc = parser.parse("[\n" +
        "  { \"source\": \"username\", \"type\": \"column\", \"value\": \"meta:username\" },\n" +
        "  { \"source\": \"email\", \"type\": \"column\", \"value\": \"meta:email\" }\n" +
        "]");

    ColumnMappingDescriptor expected = new ColumnMappingDescriptor.Builder()
        .column("username", "meta", "username")
        .column("email", "meta", "email")
        .build();

    assertEquals(expected, desc);
  }

}
