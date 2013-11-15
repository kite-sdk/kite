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
package com.cloudera.cdk.morphline.maxmind;

import java.io.File;
import java.net.InetAddress;

import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.cdk.morphline.api.AbstractMorphlineTest;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Notifications;
import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;

public class MaxmindMorphlineTest extends AbstractMorphlineTest {

  private static final String RESOURCES_DIR = "target/test-classes";

  @Test
  public void testRawMaxMindAPI() throws Exception {
    InetAddress HOST = InetAddress.getByName("128.101.101.101");    
    File database = new File(RESOURCES_DIR + "/GeoLite2-City.mmdb");
//  File database = new File(RESOURCES_DIR + "/GeoLite2-Country.mmdb");

    DatabaseReader dbReader = new DatabaseReader.Builder(database).build();
    CityResponse cityResponse = dbReader.city(HOST);

    assertEquals("US", cityResponse.getCountry().getIsoCode());
    assertEquals("United States", cityResponse.getCountry().getName());
    assertEquals("美国", cityResponse.getCountry().getNames().get("zh-CN"));

    assertEquals("Minnesota", cityResponse.getMostSpecificSubdivision().getName());
    assertEquals("MN", cityResponse.getMostSpecificSubdivision().getIsoCode());

    assertEquals("Minneapolis", cityResponse.getCity().getName());
    assertEquals("55455", cityResponse.getPostal().getCode());

    assertEquals(new Double(44.9733), cityResponse.getLocation().getLatitude());
    assertEquals(new Double(-93.2323), cityResponse.getLocation().getLongitude());

    CountryResponse countryResponse = dbReader.country(HOST);
    //System.out.println("countryResponse="+countryResponse.toString());
    assertEquals("US", countryResponse.getCountry().getIsoCode());
    assertEquals("United States", countryResponse.getCountry().getName());

    Reader reader = new Reader(database);
    JsonNode root = reader.get(HOST);
    //System.out.println("json="+root.toString());
    assertEquals("US", root.get("country").get("iso_code").textValue());
    assertEquals("United States", root.get("country").get("names").get("en").textValue());
    assertEquals("美国", root.get("country").get("names").get("zh-CN").textValue());
    
    assertEquals("Minnesota", root.get("subdivisions").get(0).get("names").get("en").textValue());
    assertEquals("MN", root.get("subdivisions").get(0).get("iso_code").textValue());
    
    assertEquals("Minneapolis", root.get("city").get("names").get("en").textValue());
    assertEquals("55455", root.get("postal").get("code").textValue());
    
    assertEquals(44.9733, root.get("location").get("latitude").doubleValue(), 0.00001);
    assertEquals(-93.2323, root.get("location").get("longitude").doubleValue(), 0.00001);    
    reader.close();
  }
  
  @Test
  public void testBasic() throws Exception {
    morphline = createMorphline("test-morphlines/geoIP");    
    
    Record record = new Record();
    String msg = "128.101.101.101";
    record.put("ip", msg);
    Record expected = new Record();
    expected.put("ip", msg);
    expected.put("/country/iso_code", "US");
    expected.put("/country/names/en", "United States");
    expected.put("/country/names/zh-CN", "美国");
    expected.put("/subdivisions[]/names/en", "Minnesota");
    expected.put("/subdivisions[]/iso_code", "MN");
    expected.put("/city/names/en", "Minneapolis");
    expected.put("/postal/code", "55455");
    expected.put("/location/latitude", 44.9733);
    expected.put("/location/longitude", -93.2323);
    
    processAndVerifySuccess(record, expected, false);
    Notifications.notifyShutdown(morphline);
  }
  
  private void processAndVerifySuccess(Record input, Record expected, boolean isSame) {
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(input));
    collector.getFirstRecord().removeAll(Fields.ATTACHMENT_BODY);
    assertEquals(expected, collector.getFirstRecord());
    if (isSame) {
      assertSame(input, collector.getFirstRecord());    
    } else {
      assertNotSame(input, collector.getFirstRecord());    
    }
  }

  @Test
  @Ignore
  // Before running this disable debug logging 
  // via log4j.logger.com.cloudera.cdk.morphline=INFO in log4j.properties
  public void benchmark() throws Exception {
    String morphlineConfigFile = "test-morphlines/geoIP";
    long durationSecs = 20;
    System.out.println("Now benchmarking " + morphlineConfigFile + " ...");
    morphline = createMorphline(morphlineConfigFile);    
    
    long start = System.currentTimeMillis();
    long duration = durationSecs * 1000;
    int iters = 0; 
    while (System.currentTimeMillis() < start + duration) {
      Record record = new Record();
      String msg = "128.101.101.101";
      record.put("ip", msg);
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));    
      iters++;
    }
    float secs = (System.currentTimeMillis() - start) / 1000.0f;
    System.out.println("Results: iters=" + iters + ", took[secs]=" + secs + ", iters/secs=" + (iters/secs));
  }  

}
