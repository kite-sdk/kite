/*
 * Copyright 2016 Cloudera Inc.
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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.stdlib.ConvertTimestampBuilder.ConvertTimestamp.DateUtil;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.ExceptionHandler;
import org.kitesdk.morphline.api.MorphlineContext;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;

public class ConvertTimestampInsertYearTest extends AbstractMorphlineTest {

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
  
  @Test
  public void testMorphlineContext() throws Exception {
    ExceptionHandler ex = new ExceptionHandler() {      
      @Override
      public void handleException(Throwable t, Record record) {
        throw new RuntimeException(t);        
      }
    };
    
    MetricRegistry metricRegistry = new MetricRegistry();
    metricRegistry.register("myCounter", new Counter());
    
    HealthCheckRegistry healthChecks = new HealthCheckRegistry();
    healthChecks.register("foo", new HealthCheck() {      
      @Override
      protected Result check() throws Exception {
        return Result.healthy("flawless");
      }
    });

    Map<String,Object> settings = new HashMap<String,Object>(); 
    
    MorphlineContext ctx = new MorphlineContext.Builder()
      .setSettings(settings)
      .setExceptionHandler(ex)
      .setHealthCheckRegistry(healthChecks)
      .setMetricRegistry(metricRegistry)
      .build();
    
    assertSame(settings, ctx.getSettings());
    assertSame(ex, ctx.getExceptionHandler());
    assertSame(metricRegistry, ctx.getMetricRegistry());
    assertSame(healthChecks, ctx.getHealthCheckRegistry());
    ctx.getHealthCheckRegistry().runHealthChecks();
    
    assertEquals(0, new MorphlineContext.Builder().build().getSettings().size());
  }
  
  
  @Test
  public void testConvertTimestampWithMissingYear() throws Exception {
    //Testing two configurations (ts1 and ts2), the first uses the missing year offset of 0
    //The second uses the missing year offset of -5 (11 months in the past, 1 month in the future)
    //Loop through 12 months, build ts1 and ts2 and test that Morphline succesfully infers the correct year.
    for (int i=-5; i<=6; i++) {
      SimpleDateFormat inputFormat = new SimpleDateFormat("MMM dd HH:mm:ss");
      SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      
      Date date = new Date(System.currentTimeMillis());
      Calendar calTs1 = Calendar.getInstance();
      calTs1.setTime(date);
      calTs1.setTimeZone(TimeZone.getTimeZone("UTC"));
      calTs1.add(Calendar.MONTH, i);
      
      Calendar calTs2 = Calendar.getInstance();
      calTs2.setTime(date);
      calTs2.setTimeZone(TimeZone.getTimeZone("UTC"));
      calTs2.add(Calendar.MONTH, i-5);
      
      Calendar calTs3 = Calendar.getInstance();
      calTs3.setTime(date);
      calTs3.setTimeZone(TimeZone.getTimeZone("UTC"));
      calTs3.add(Calendar.MONTH, i);
      calTs3.add(Calendar.YEAR, -5);
      
      //Small tweak to avoid the 1 month in the future ticking over by a few seconds between now
      //and when the insertYear method actually runs
      if (i==6) {
        calTs1.add(Calendar.DAY_OF_MONTH, -1);
        calTs2.add(Calendar.DAY_OF_MONTH, -1);
        calTs3.add(Calendar.DAY_OF_MONTH, -1);
      }
      
      morphline = createMorphline("test-morphlines/convertTimestampInsertYear");    

      Record record = new Record();
      record.put("ts1", inputFormat.format(calTs1.getTime()));
      record.put("ts2", inputFormat.format(calTs2.getTime()));
      record.put("ts3", inputFormat.format(calTs3.getTime()));
      
      Record expected = new Record();
      expected.put("ts1", outputFormat.format(calTs1.getTime()));
      expected.put("ts2", outputFormat.format(calTs2.getTime()));
      expected.put("ts3", outputFormat.format(calTs3.getTime()));

      processAndVerifySuccess(record, expected);
      
    }
  }

  @Test
  public void testConvertTimestamp() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestamp");    
    Record record = new Record();
    record.put("ts1", "2011-09-06T14:14:34.789Z"); // "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    record.put("ts1", "2012-09-06T14:14:34"); 
    record.put("ts1", "2013-09-06");
    Record expected = new Record();
    expected.put("ts1", "2011-09-06T07:14:34.789-0700");
    expected.put("ts1", "2012-09-06T07:14:34.000-0700");
    expected.put("ts1", "2013-09-05T17:00:00.000-0700");
    processAndVerifySuccess(record, expected);
  }
  
 @Test
  public void testInsertYear() throws Exception {
	  SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ssZZZ");
	  
	  Date inputDate;
	  Date currentDate; 
	  Date expectedDate; 
    Date outputDate; 
    int targetYear;
    
    //Test year rollover scenario just after rollover
    inputDate = sdf.parse("01/01/2011 01:02:03UTC");
    currentDate = sdf.parse("01/01/2011 01:02:03UTC");
	  expectedDate = sdf.parse("01/01/2011 01:02:03UTC");
	  targetYear = 2011;
    outputDate = DateUtil.insertYear(inputDate, currentDate, -5, targetYear, TimeZone.getTimeZone("UTC"));
	  
    assertEquals(expectedDate.toString(), outputDate.toString());
    
    //Test year rollover scenario where date is just before Jan 1st
    inputDate = sdf.parse("31/12/2011 23:02:03UTC");
    currentDate = sdf.parse("01/01/2011 01:02:03UTC");
    expectedDate = sdf.parse("31/12/2010 23:02:03UTC");
    targetYear = 2011;
    outputDate = DateUtil.insertYear(inputDate, currentDate, -5, targetYear, TimeZone.getTimeZone("UTC"));
    
    assertEquals(expectedDate.toString(), outputDate.toString());
    
    //Test year rollover scenario where system clock is slow
    inputDate = sdf.parse("01/01/2011 01:02:03UTC");
    currentDate = sdf.parse("31/12/2010 23:02:03UTC");
    expectedDate = sdf.parse("01/01/2011 01:02:03UTC");
    targetYear = 2010;
    outputDate = DateUtil.insertYear(inputDate, currentDate, -5, targetYear, TimeZone.getTimeZone("UTC"));
    
    assertEquals(expectedDate.toString(), outputDate.toString());
    
    //Test year rollover scenario just before rollover
    inputDate = sdf.parse("31/12/2010 23:02:03UTC");
    currentDate = sdf.parse("31/12/2010 23:02:03UTC");
    expectedDate = sdf.parse("31/12/2010 23:02:03UTC");
    targetYear = 2010;
    outputDate = DateUtil.insertYear(inputDate, currentDate, -5, targetYear, TimeZone.getTimeZone("UTC"));
    
    assertEquals(expectedDate.toString(), outputDate.toString());
    
    //Test mid year
    inputDate = sdf.parse("31/6/2010 23:02:03UTC");
    currentDate = sdf.parse("31/6/2010 23:02:03UTC");
    expectedDate = sdf.parse("31/6/2010 23:02:03UTC");
    targetYear = 2010;
    outputDate = DateUtil.insertYear(inputDate, currentDate, -5, targetYear, TimeZone.getTimeZone("UTC"));
    
    assertEquals(expectedDate.toString(), outputDate.toString());
    
    //Test mid year with lagging message
    inputDate = sdf.parse("31/8/2010 23:02:03UTC");
    currentDate = sdf.parse("31/6/2010 23:02:03UTC");
    expectedDate = sdf.parse("31/8/2009 23:02:03UTC");
    targetYear = 2010;
    outputDate = DateUtil.insertYear(inputDate, currentDate, -5, targetYear, TimeZone.getTimeZone("UTC"));
    
    assertEquals(expectedDate.toString(), outputDate.toString());
    
    //Test mid year with future message
    inputDate = sdf.parse("21/7/2010 23:02:03UTC");
    currentDate = sdf.parse("31/6/2010 23:02:03UTC");
    expectedDate = sdf.parse("21/7/2010 23:02:03UTC");
    targetYear = 2010;
    outputDate = DateUtil.insertYear(inputDate, currentDate, -5, targetYear, TimeZone.getTimeZone("UTC"));
    
    assertEquals(expectedDate.toString(), outputDate.toString());
    
    //Test leap year scenario
    inputDate = sdf.parse("29/2/2016 23:02:03UTC");
    currentDate = sdf.parse("29/2/2016 23:02:03UTC");
    expectedDate = sdf.parse("29/2/2016 23:02:03UTC");
    targetYear = 2016;
    outputDate = DateUtil.insertYear(inputDate, currentDate, -5, targetYear, TimeZone.getTimeZone("UTC"));
    
    assertEquals(expectedDate.toString(), outputDate.toString());
    
  }
  
}
