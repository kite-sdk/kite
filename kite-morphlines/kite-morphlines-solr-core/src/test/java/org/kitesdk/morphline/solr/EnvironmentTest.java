/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.morphline.solr;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;

import org.apache.commons.lang.SystemUtils;
import org.apache.lucene.LucenePackage;
import org.apache.lucene.util.Version;
import org.apache.solr.core.SolrCore;
import org.junit.Assert;
import org.junit.Test;

/** Print and verify some info about the environment in which the unit tests are running */
public class EnvironmentTest extends Assert {

  // see pom.xml, example: "4.2.0"
  private static final String EXPECTED_SOLR_VERSION;
  
  static {
    String version = System.getProperty("solr.expected.version");
    if (version != null) {
      int i = version.indexOf('-');
      if (i >= 0) {
        // e.g. strip off "-cdh5.4.0-SNAPSHOT" from "4.10.3-cdh5.4.0-SNAPSHOT"
        version = version.substring(0, i); 
      }
    }
    EXPECTED_SOLR_VERSION = version;
  }

  @Test
  public void testEnvironment() throws UnknownHostException {
    System.out.println("EXPECTED_SOLR_VERSION: " + EXPECTED_SOLR_VERSION);

    System.out.println("Running test suite with java version: " + SystemUtils.JAVA_VERSION + " "
        + SystemUtils.JAVA_VM_NAME + " on " + SystemUtils.OS_NAME + " " + SystemUtils.OS_VERSION + "/"
        + SystemUtils.OS_ARCH + " on host: " + InetAddress.getLocalHost().getHostName());

    Package p = SolrCore.class.getPackage();
    System.out.println("Running test suite with solr-spec-version: " + p.getSpecificationVersion()
        + ", solr-impl-version: " + p.getImplementationVersion());
    if (EXPECTED_SOLR_VERSION != null) {
      assertTrue("unexpected version: " + p.getSpecificationVersion(),
          p.getSpecificationVersion().startsWith(EXPECTED_SOLR_VERSION));
      assertTrue("unexpected version: " + p.getImplementationVersion(),
          p.getImplementationVersion().startsWith(EXPECTED_SOLR_VERSION));
    }

    p = LucenePackage.class.getPackage();
    System.out.println("Running test suite with lucene-spec-version: " + p.getSpecificationVersion()
        + ", lucene-impl-version: " + p.getImplementationVersion());
    if (EXPECTED_SOLR_VERSION != null) {
      assertTrue("unexpected version: " + p.getSpecificationVersion(),
          p.getSpecificationVersion().startsWith(EXPECTED_SOLR_VERSION));
      assertTrue("unexpected version: " + p.getImplementationVersion(),
          p.getImplementationVersion().startsWith(EXPECTED_SOLR_VERSION));

      Version expectedMinorLuceneVersion = getMinorLuceneVersion(EXPECTED_SOLR_VERSION);
      System.out.println("expectedMinorLuceneVersion: " + expectedMinorLuceneVersion);
      assertTrue(Version.LATEST.onOrAfter(expectedMinorLuceneVersion));
    }
  }

  private static Version getMinorLuceneVersion(String version) {
    try {
      return Version.parseLeniently(version.replaceFirst("^(\\d)\\.(\\d).*", "LUCENE_$1$2"));
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

}
