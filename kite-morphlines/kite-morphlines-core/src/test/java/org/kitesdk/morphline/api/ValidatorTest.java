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

import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.morphline.base.Validator;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ValidatorTest extends Assert {
  
  @Test
  public void testValidatorEnums() {
    Config empty = ConfigFactory.empty();
    NumRequiredMatches param;
    
    param = null;
    param = new Validator<NumRequiredMatches>().validateEnum(empty, "all", NumRequiredMatches.class);
    assertEquals(param, NumRequiredMatches.all);

    param = null;
    param = new Validator<NumRequiredMatches>().validateEnum(empty, "all", NumRequiredMatches.class,
        NumRequiredMatches.all);
    assertEquals(param, NumRequiredMatches.all);
    
    param = null;
    try {
      param = new Validator<NumRequiredMatches>().validateEnum(empty, "all", NumRequiredMatches.class,
          NumRequiredMatches.atLeastOnce, NumRequiredMatches.once);
      fail();
    } catch (MorphlineCompilationException e) {
      ; // 
    }
  }
  
  @Test
  public void testValidatorRanges() {
    Config empty = ConfigFactory.empty();
    
    new Validator<Integer>().validateRange(empty, 5, 0, 10);

    try {
      new Validator<Integer>().validateRange(empty, 5, 9, 10);
      fail();
    } catch (MorphlineCompilationException e) {
      ; // 
    }
    
    try {
      new Validator<Integer>().validateRange(empty, 15, 9, 10);
      fail();
    } catch (MorphlineCompilationException e) {
      ; // 
    }

  }
  
  private static enum NumRequiredMatches {
    atLeastOnce,
    once,
    all     
  }     

//  @Test
//  public void testValidatorChoices() {
//    Config empty = ConfigFactory.empty();
//    String param;
//    
//    param = null;
//    param = new Validator<String>().validateChoice(empty, "all", "all", "allAtOnce", "once");
//    assertEquals(param, "all");
//
//    param = null;
//    try {
//      param = new Validator<String>().validateChoice(empty, "XXX", "all", "allAtOnce", "once");
//      fail();
//    } catch (MorphlineParsingException e) {
//      ; // 
//    }
//  }
  
}
