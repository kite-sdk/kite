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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValueFactory;

/** See https://github.com/typesafehub/config */
public class SimpleHoconConfigTest extends Assert {

  private static final String TIKA_CONFIG_LOCATION = "tika.config";
  
  @Test
	@Ignore
	public void testBasic() {
		Config conf = ConfigFactory.load("test-application").getConfig(getClass().getPackage().getName() + ".test");
		
    assertEquals(conf.getString("foo.bar"), "1234");
    assertEquals(conf.getInt("foo.bar"), 1234);
		//assertEquals(conf.getInt("moo.bar"), 56789); // read from reference.config
		
		Config subConfig = conf.getConfig("foo");
    assertNotNull(subConfig);
    assertEquals(subConfig.getString("bar"), "1234");
    
    assertFalse(conf.hasPath("missing.foox.barx"));
    try {
      conf.getString("missing.foox.barx");
      fail("Failed to detect missing param");
    } catch (ConfigException.Missing e) {} 

    Iterator userNames = Arrays.asList("nadja", "basti").iterator();
		Iterator passwords = Arrays.asList("nchangeit", "bchangeit").iterator();
		for (Config user : conf.getConfigList("users")) {
			assertEquals(user.getString("userName"), userNames.next());
			assertEquals(user.getString("password"), passwords.next());
		}
		assertFalse(userNames.hasNext());
		assertFalse(passwords.hasNext());
		
		assertEquals(conf.getStringList("files.paths"), Arrays.asList("dir/file1.log", "dir/file2.txt"));
		Iterator schemas = Arrays.asList("schema1.json", "schema2.json").iterator();
		Iterator globs = Arrays.asList("*.log*", "*.txt*").iterator();
		for (Config fileMapping : conf.getConfigList("files.fileMappings")) {
			assertEquals(fileMapping.getString("schema"), schemas.next());
			assertEquals(fileMapping.getString("glob"), globs.next());
		}
		assertFalse(schemas.hasNext());
		assertFalse(globs.hasNext());    
				
//		Object list2 = conf.entrySet();
//		Object list2 = conf.getAnyRef("users.userName");
//		assertEquals(conf.getString("users.user.userName"), "nadja");
	}
	
  @Test
	public void testParseMap() { // test access based on path
    final Map<String, String> map = Maps.newHashMap();
    map.put(TIKA_CONFIG_LOCATION, "src/test/resources/tika-config.xml");
    map.put("collection1.testcoll.solr.home", "target/test-classes/solr/collection1");
//    Config config = ConfigValueFactory.fromMap(new Context(map).getParameters()).toConfig();
    Config config = ConfigFactory.parseMap(map);
	  String filePath = config.getString(TIKA_CONFIG_LOCATION);
	  assertEquals(map.get(TIKA_CONFIG_LOCATION), filePath);
    Config subConfig = config.getConfig("collection1").getConfig("testcoll");
    assertEquals("target/test-classes/solr/collection1", subConfig.getString("solr.home"));
	}
  
  @Test
  public void testFromMap() { // test access based on key
    final Map<String, String> map = Maps.newHashMap();
    map.put(TIKA_CONFIG_LOCATION, "src/test/resources/tika-config.xml");
    String key = "collection1.testcoll.solr.home";
    map.put(key, "target/test-classes/solr/collection1");
    ConfigObject config = ConfigValueFactory.fromMap(map);
    String filePath = config.get(TIKA_CONFIG_LOCATION).unwrapped().toString();
    assertEquals(map.get(TIKA_CONFIG_LOCATION), filePath);
    assertEquals(map.get(key), config.get(key).unwrapped().toString());
  }
  
  @Test
  public void testCacheBuilder() throws ExecutionException {
    LoadingCache<String, Matcher> cache = CacheBuilder.newBuilder()
        .maximumSize(10)
        .build(
            new CacheLoader<String, Matcher>() {
              public Matcher load(String key) {
                return Pattern.compile(key).matcher("");
              }
            });
    
    Matcher m = cache.get(".*");
    Matcher m2 = cache.get(".*");
  }

}
