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

import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.shaded.org.apache.commons.codec.binary.binary.Base64;

import com.typesafe.config.Config;

/**
 * Command that converts a Base64 encoded String to a byte[] per Section <cite>6.8. Base64
 * Content-Transfer-Encoding</cite> of <a href="http://www.ietf.org/rfc/rfc2045.txt">RFC 2045</a>.
 */
public final class DecodeBase64Builder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("decodeBase64");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Base64Decode(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Base64Decode extends AbstractFieldTransformCommand {

    private final Base64 base64 = new Base64();
    
    public Base64Decode(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      validateArguments();
    }
        
    @Override
    protected Object transformFieldValue(Object value) {
      String str = value.toString();
      // consider switching to com.fasterxml.jackson.core.Base64Variants.MIME.decode(String, ByteArrayBuilder) for performance?
      byte[] bytes = base64.decode(str);
      return bytes;
    }
    
  }
 
}
