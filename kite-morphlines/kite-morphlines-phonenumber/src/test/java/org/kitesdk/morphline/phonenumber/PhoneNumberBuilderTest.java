/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
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
package org.kitesdk.morphline.phonenumber;

import org.junit.After;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Notifications;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

public class PhoneNumberBuilderTest extends AbstractMorphlineTest {

  @After
  public void tearDown() {
    if (morphline != null) {
      Notifications.notifyShutdown(morphline);
      Notifications.notifyShutdown(morphline);
      morphline = null;
    }
  }

  @Test(expected = ConfigException.Missing.class)
  public void cannotBuildWithoutInputField() {
    buildCommand(null, "prefix_", "US", "region_code", "INTERNATIONAL");
  }

  @Test(expected = MorphlineCompilationException.class)
  public void cannotBuildWithBadRegionCode() {
    buildCommand("input", "prefix_", "BAD REGION", "region_code", "INTERNATIONAL");
  }

  @Test(expected = MorphlineCompilationException.class)
  public void cannotBuildWithBadNumberFormat() {
    buildCommand("input", "prefix_", "US", "region_code", "XXXX");
  }

  @Test
  public void canBuildWithMinimalConfig() {
    buildCommand("input", null, null, null, null);
    assertTrue(true);
  }

  @Test
  public void simpleTest() {
    this.morphline = buildCommand("input", null, null, null, null);

    final Record record = new Record();
    record.put("input", "+3461 91 234 56");

    final Record expected = record.copy();
    expected.put("phone_phone", "+34 619 12 34 56");
    expected.put("phone_country_code", "34");
    expected.put("phone_region_code", "ES");

    processAndVerifySuccess(record, expected, true);
  }

  @Test
  public void alternativeOutputPrefix() {
    this.morphline = buildCommand("input", "myprefix_", null, null, null);

    final Record record = new Record();
    record.put("input", "+3461 91 234 56");

    final Record expected = record.copy();
    expected.put("myprefix_phone", "+34 619 12 34 56");
    expected.put("myprefix_country_code", "34");
    expected.put("myprefix_region_code", "ES");

    processAndVerifySuccess(record, expected, true);
  }

  @Test
  public void toRFC3966Format() {
    this.morphline = buildCommand("input", null, null, null, "rfc3966");

    final Record record = new Record();
    record.put("input", "+3461 91 234 56");

    final Record expected = record.copy();
    expected.put("phone_phone", "tel:+34-619-12-34-56");
    expected.put("phone_country_code", "34");
    expected.put("phone_region_code", "ES");

    processAndVerifySuccess(record, expected, true);
  }

  @Test
  public void nullInput() {
    this.morphline = buildCommand("input", null, null, null, null);
    processAndVerifySuccess(new Record(), null, false);
  }

  @Test
  public void nonStringInput() {
    this.morphline = buildCommand("input", null, null, null, null);
    final Record record = new Record();
    record.put("input", 1L);
    processAndVerifySuccess(record, null, false);
  }

  @Test
  public void unparseableNumber() {
    this.morphline = buildCommand("input", null, null, null, null);
    final Record record = new Record();
    record.put("input", "NON PARSEABLE NUMBER");
    processAndVerifySuccess(record, null, false);
  }

  @Test
  public void customRegionCodeField() {
    this.morphline = buildCommand("input", null, null, "region_code", null);

    final Record record = new Record();
    record.put("input", "003461 91 234 56");
    record.put("region_code", "ES");

    final Record expected = record.copy();
    expected.put("phone_phone", "+34 619 12 34 56");
    expected.put("phone_country_code", "34");
    expected.put("phone_region_code", "ES");

    processAndVerifySuccess(record, expected, true);
  }

  @Test
  public void customRegionCodeFieldWithFallback() {
    this.morphline = buildCommand("input", null, null, "region_code", null);

    final Record record = new Record();
    record.put("input", "003461 91 234 56");

    final Record expected = record.copy();
    expected.put("phone_phone", "+1 0034619123456");
    expected.put("phone_country_code", "1");
    expected.put("phone_region_code", "US");

    processAndVerifySuccess(record, expected, true);
  }

  @Test
  public void customRegionCodeFieldWithTypeErrorFallback() {
    this.morphline = buildCommand("input", null, null, "region_code", null);

    final Record record = new Record();
    record.put("input", "+3461 91 234 56");
    record.put("region_code", 1L);

    final Record expected = record.copy();
    expected.put("phone_phone", "+34 619 12 34 56");
    expected.put("phone_country_code", "34");
    expected.put("phone_region_code", "ES");

    processAndVerifySuccess(record, expected, true);
  }

  @Test
  public void customRegionCodeFieldWithInvalidRegionCode() {
    this.morphline = buildCommand("input", null, "ES", "region_code", null);

    final Record record = new Record();
    record.put("input", "3461 91 234 56");
    record.put("region_code", "XXXX");

    final Record expected = record.copy();
    expected.put("phone_phone", "+34 619 12 34 56");
    expected.put("phone_country_code", "34");
    expected.put("phone_region_code", "ES");

    processAndVerifySuccess(record, expected, true);
  }

  private Command buildCommand(
      final String inputField,
      final String outputFieldPrefix,
      final String defaultRegion,
      final String regionCodeField,
      final String phoneFormat
      ) {
    final StringBuilder configString = new StringBuilder();
    if (inputField != null) {
      configString.append("inputField=\"").append(inputField).append("\"\n");
    }
    if (outputFieldPrefix != null) {
      configString.append("outputFieldPrefix=\"").append(outputFieldPrefix).append("\"\n");
    }
    if (defaultRegion != null) {
      configString.append("defaultRegionCode=\"").append(defaultRegion).append("\"\n");
    }
    if (regionCodeField != null) {
      configString.append("regionCodeField=\"").append(regionCodeField).append("\"\n");
    }
    if (phoneFormat != null) {
      configString.append("outputPhoneFormat=\"").append(phoneFormat).append("\"\n");
    }
    final Config config = ConfigFactory.parseString(configString.toString());
    return new PhoneNumberBuilder().build(config, new RootCommand(), collector, new MorphlineContext.Builder().build());
  }

   private void processAndVerifySuccess(final Record input, final Record expected, final boolean isSuccess) {
     final Record inputCopy = input.copy();
     collector.reset();
     startSession();
     assertEquals(1, collector.getNumStartEvents());
     assertEquals(isSuccess, morphline.process(inputCopy));
     if (!isSuccess) {
       assertEquals(0, collector.getRecords().size());
     } else {
       assertEquals(1, collector.getRecords().size());
       final Record actual = collector.getFirstRecord();
       assertEquals(expected, actual);
     }
   }


  private static final class RootCommand implements Command {

    @Override
    public Command getParent() {
      return null;
    }

    @Override
    public void notify(Record notification) {
      throw new UnsupportedOperationException("Root command should be invisible and must not be called");
    }

    @Override
    public boolean process(Record record) {
      throw new UnsupportedOperationException("Root command should be invisible and must not be called");
    }

  }

}
