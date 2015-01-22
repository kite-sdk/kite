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
package org.kitesdk.morphline.maxmind;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class MaxmindMorphlineTest extends AbstractMorphlineTest {

  private Command buildCommand(final String inputField, String db) {
    final Config config = ConfigFactory.parseString(
        String.format("inputField=%s\ndatabase=target/test-classes/%s\n", inputField, db)
    );
    return new GeoIPBuilder().build(config, new RootCommand(), collector, new MorphlineContext.Builder().build());
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (morphline != null) {
      Notifications.notifyShutdown(morphline);
      Notifications.notifyShutdown(morphline);
      morphline = null;
    }
    super.tearDown();
  }

  @Test
  public void testIPv4CountryOnly() throws Exception {
    morphline = buildCommand("ip", "GeoLite2-Country.mmdb");
    final Record record = new Record();
    final String ip = "128.101.101.101";
    record.put("ip", ip);
    processAndVerifySuccess2(record, true, false, 6252001, null);
  }

  @Test
  public void testNotFoundIPv4() throws Exception {
    morphline = buildCommand("ip", "GeoLite2-City.mmdb");
    final Record record = new Record();
    final String ip = "127.0.0.1";
    record.put("ip", ip);
    processAndVerifySuccess2(record, false, false, null, null);
  }

  @Test
  public void testBadData() throws Exception {
    morphline = buildCommand("ip", "GeoLite2-City.mmdb");
    final Record record = new Record();
    final String ip = "DAT DATA";
    record.put("ip", ip);
    processAndVerifySuccess2(record, false, false, null, null);
  }

  @Test
  public void testEmpty() throws Exception {
    morphline = buildCommand("ip", "GeoLite2-City.mmdb");
    final Record record = new Record();
    processAndVerifySuccess2(record, false, false, null, null);
  }

  @Test
  public void testIPv6CountryOnly() throws Exception {
    morphline = buildCommand("ip", "GeoLite2-Country.mmdb");
    final Record record = new Record();
    final String ip = "2001:4860:4860::8888";
    record.put("ip", ip);
    processAndVerifySuccess2(record, true, false, 6252001, null);
  }

  @Test
  public void testNotFoundIPv6() throws Exception {
    morphline = buildCommand("ip", "GeoLite2-City.mmdb");
    final Record record = new Record();
    final String ip = "::1";
    record.put("ip", ip);
    processAndVerifySuccess2(record, false, false, null, null);
  }

  @Test(expected = MorphlineCompilationException.class)
  public void testBadDBPath() throws Exception {
    buildCommand("ip", "BAD-PATH-MMDB");
  }
  
  private void processAndVerifySuccess2(final Record input, final boolean isSuccess, boolean isSame,
      final Integer countryGeoNameId, final Integer cityGeoNameId) {
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
      final Object body = actual.getFirstValue(Fields.ATTACHMENT_BODY);
      actual.removeAll(Fields.ATTACHMENT_BODY);
      assertEquals(input, actual);
      assertTrue(body instanceof JsonNode);
      final JsonNode jsonNode = (JsonNode)body;
      assertNotNull(jsonNode);
      final JsonNode countryNode = jsonNode.get("country");
      if (countryGeoNameId != null) {
        assertNotNull(countryNode);
        assertEquals(countryGeoNameId.intValue(), countryNode.get("geoname_id").asInt());
      } else {
        assertNull(countryNode);
      }
      final JsonNode cityNode = jsonNode.get("city");
      if (cityGeoNameId != null) {
        assertNotNull(cityNode);
        assertEquals(cityGeoNameId.intValue(), cityNode.get("geoname_id").asInt());
      } else {
        assertNull(cityNode);
      }
    }
  }


  @Test
  public void testIPv4() throws Exception {
    morphline = createMorphline("test-morphlines/geoIP");

    Record record = new Record();
    String ip = "128.101.101.101";
    record.put("ip", ip);
    Record expected = new Record();
    expected.put("ip", ip);
    expected.put("/country/iso_code", "US");
    expected.put("/country/names/en", "United States");
    expected.put("/country/names/zh-CN", "美国");
    expected.put("/subdivisions[]/names/en", "Minnesota");
    expected.put("/subdivisions[]/iso_code", "MN");
    expected.put("/city/names/en", "Minneapolis");
    expected.put("/postal/code", "55455");
    expected.put("/location/latitude", 44.9733);
    expected.put("/location/longitude", -93.2323);
    expected.put("/location/latitude_longitude", "44.9733,-93.2323");
    expected.put("/location/longitude_latitude", "-93.2323,44.9733");

    processAndVerifySuccess(record, expected, false);
  }

  @Test
  public void testIPv6() throws Exception {
    morphline = createMorphline("test-morphlines/geoIP");

    Record record = new Record();
    String ip = "2001:620::1";
    record.put("ip", ip);
    Record expected = new Record();
    expected.put("ip", ip);
    expected.put("/country/iso_code", "CH");
    expected.put("/country/names/en", "Switzerland");
    expected.put("/country/names/zh-CN", "瑞士");
    expected.put("/location/latitude", 47.00016);
    expected.put("/location/longitude", 8.01427);
    expected.put("/location/latitude_longitude", "47.00016,8.01427");
    expected.put("/location/longitude_latitude", "8.01427,47.00016");

    processAndVerifySuccess(record, expected, false);
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
  // via log4j.logger.org.kitesdk.morphline=INFO in log4j.properties
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

  /*
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
    reader.close();
  }
  */

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
