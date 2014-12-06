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
package org.kitesdk.morphline.tika;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class DetectMimeTypesTest extends AbstractMorphlineTest {

  private Map<List, Command> morphlineCache = new HashMap();
  
  private static final String AVRO_MIME_TYPE = "avro/binary"; // ReadAvroContainerBuilder.MIME_TYPE;  
  
  private static final File AVRO_FILE = new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433.avro");
  private static final File JPG_FILE = new File(RESOURCES_DIR + "/test-documents/testJPEG_EXIF.jpg");
  
  @Test
  public void testDetectMimeTypesWithFile() throws Exception {
    // config file uses mimeTypesFiles : [src/test/resources/org/apache/tika/mime/custom-mimetypes.xml] 
    testDetectMimeTypesInternal("test-morphlines/detectMimeTypesWithFile");    
  }
  
  @Test
  public void testDetectMimeTypesWithString() throws Exception {
    // config file uses mimeTypesString : """ some mime types go here """ 
    testDetectMimeTypesInternal("test-morphlines/detectMimeTypesWithString");    
  }
  
  private void testDetectMimeTypesInternal(String configFile) throws Exception {
    // verify that Avro is classified as Avro  
    morphline = createMorphline(configFile);    
    Record record = new Record();    
    record.put(Fields.ATTACHMENT_BODY, Files.toByteArray(AVRO_FILE));
    startSession();
    morphline.process(record);
    assertEquals(AVRO_MIME_TYPE, collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_MIME_TYPE));

    // verify that JPG isnt' classified as JPG because this morphline uses includeDefaultMimeTypes : false 
    collector.reset();
    record = new Record();    
    record.put(Fields.ATTACHMENT_BODY, Files.toByteArray(JPG_FILE));
    startSession();
    morphline.process(record);
    assertEquals("application/octet-stream", collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_MIME_TYPE));
  }
  
  @Test
  public void testDetectMimeTypesWithDefaultMimeTypes() throws Exception {
    morphline = createMorphline("test-morphlines/detectMimeTypesWithDefaultMimeTypes");    
    Record record = new Record();    
    record.put(Fields.ATTACHMENT_BODY, Files.toByteArray(JPG_FILE));
    startSession();
    morphline.process(record);
    assertEquals("image/jpeg", collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_MIME_TYPE));
  }

  @Test
  public void testMimeTypeAlreadySpecifiedOnInputRemainsUnchanged() throws Exception {
    morphline = createMorphline("test-morphlines/detectMimeTypesWithDefaultMimeTypes");    
    Record record = new Record();    
    record.put(Fields.ATTACHMENT_BODY, Files.toByteArray(JPG_FILE));
    record.put(Fields.ATTACHMENT_MIME_TYPE, "foo/bar");
    startSession();
    morphline.process(record);
    assertEquals("foo/bar", collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_MIME_TYPE));
  }

  @Test
  public void testPlainText() throws Exception { 
    Record event = createEvent("foo".getBytes("UTF-8"));
    assertEquals("text/plain", detect(event, false));
  }

  @Test
  public void testUnknownType() throws Exception {    
    Record event = createEvent(new byte[] {3, 4, 5, 6});
    assertEquals("application/octet-stream", detect(event, false));
  }

  @Test
  public void testUnknownEmptyType() throws Exception {    
    Record event = createEvent(new byte[0]);
    assertEquals("application/octet-stream", detect(event, false));
  }

  @Ignore
  @Test
  public void testNullType() throws Exception {    
    Record event = createEvent(null);
    assertEquals("application/octet-stream", detect(event, false));
  }

  @Test
  public void testXML() throws Exception {    
    Record event = createEvent("<?xml version=\"1.0\"?><foo/>".getBytes("UTF-8"));
    assertEquals("application/xml", detect(event, false));
  }

  public void testXML11() throws Exception {    
    Record event = createEvent("<?xml version=\"1.1\"?><foo/>".getBytes("UTF-8"));
    assertEquals("application/xml", detect(event, false));
  }

  public void testXMLAnyVersion() throws Exception {    
    Record event = createEvent("<?xml version=\"\"?><foo/>".getBytes("UTF-8"));
    assertEquals("application/xml", detect(event, false));
  }

  @Test
  public void testXMLasTextPlain() throws Exception {    
    Record event = createEvent("<foo/>".getBytes("UTF-8"));
    assertEquals("text/plain", detect(event, false));
  }

  @Test
  public void testVariousFileTypes() throws Exception {    
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testBMPfp.txt", "text/plain", "text/plain", 
        path + "/boilerplate.html", "application/xhtml+xml", "application/xhtml+xml",
        path + "/NullHeader.docx", "application/zip", "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        path + "/testWORD_various.doc", "application/x-tika-msoffice", "application/msword",         
        path + "/testPDF.pdf", "application/pdf", "application/pdf",
        path + "/testJPEG_EXIF.jpg", "image/jpeg", "image/jpeg",
        path + "/testXML.xml", "application/xml", "application/xml",
        path + "/cars.tsv", "text/plain", "text/tab-separated-values",
        path + "/cars.ssv", "text/plain", "text/space-separated-values",
        path + "/cars.csv", "text/plain", "text/csv",
        path + "/cars.csv.gz", "application/x-gzip", "application/x-gzip",
        path + "/cars.tar.gz", "application/x-gzip", "application/x-gzip",
        path + "/sample-statuses-20120906-141433.avro", "avro/binary", "avro/binary",
        
        path + "/testPPT_various.ppt", "application/x-tika-msoffice", "application/vnd.ms-powerpoint",
        path + "/testPPT_various.pptx", "application/x-tika-ooxml", "application/vnd.openxmlformats-officedocument.presentationml.presentation",        
        path + "/testEXCEL.xlsx", "application/x-tika-ooxml", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        path + "/testEXCEL.xls", "application/vnd.ms-excel", "application/vnd.ms-excel",
        path + "/testPages.pages",  "application/zip", "application/vnd.apple.pages",
//        path + "/testNumbers.numbers", "application/zip", "application/vnd.apple.numbers",
//        path + "/testKeynote.key", "application/zip", "application/vnd.apple.keynote",
        
        path + "/testRTFVarious.rtf", "application/rtf", "application/rtf",
        path + "/complex.mbox", "text/plain", "application/mbox",        
        path + "/test-outlook.msg", "application/x-tika-msoffice", "application/vnd.ms-outlook",
        path + "/testEMLX.emlx", "message/x-emlx", "message/x-emlx",
        path + "/testRFC822",  "message/rfc822", "message/rfc822",
        path + "/rsstest.rss", "application/rss+xml", "application/rss+xml",
        path + "/testDITA.dita", "application/dita+xml; format=task", "application/dita+xml; format=task",
        
        path + "/testMP3i18n.mp3", "audio/mpeg", "audio/mpeg",
        path + "/testAIFF.aif", "audio/x-aiff", "audio/x-aiff",
        path + "/testFLAC.flac", "audio/x-flac", "audio/x-flac",
        path + "/testFLAC.oga", "audio/ogg", "audio/ogg",
        path + "/testVORBIS.ogg",  "audio/ogg", "audio/ogg",
        path + "/testMP4.m4a", "audio/mp4", "audio/mp4",
        path + "/testWAV.wav",  "audio/x-wav", "audio/x-wav",
        path + "/testWMA.wma",  "audio/x-ms-wma", "audio/x-ms-wma",
        
        path + "/testFLV.flv", "video/x-flv", "video/x-flv",
        path + "/testWMV.wmv",  "video/x-ms-wmv", "video/x-ms-wmv",
        
        path + "/testBMP.bmp", "image/x-ms-bmp", "image/x-ms-bmp",
        path + "/testPNG.png", "image/png", "image/png",        
        path + "/testPSD.psd", "image/vnd.adobe.photoshop", "image/vnd.adobe.photoshop",        
        path + "/testSVG.svg", "image/svg+xml", "image/svg+xml",        
        path + "/testTIFF.tif", "image/tiff", "image/tiff",        

        path + "/test-documents.7z",  "application/x-7z-compressed", "application/x-7z-compressed",
        path + "/test-documents.cpio",  "application/x-cpio", "application/x-cpio",
        path + "/test-documents.tar",  "application/x-gtar", "application/x-gtar",
        path + "/test-documents.tbz2",  "application/x-bzip2", "application/x-bzip2",
        path + "/test-documents.tgz",  "application/x-gzip", "application/x-gzip",
        path + "/test-documents.zip",  "application/zip", "application/zip",
        path + "/test-zip-of-zip.zip",  "application/zip", "application/zip",
        path + "/testJAR.jar",  "application/zip", "application/java-archive",
        
        path + "/testKML.kml",  "application/vnd.google-earth.kml+xml", "application/vnd.google-earth.kml+xml",
        path + "/testRDF.rdf",  "application/rdf+xml", "application/rdf+xml",
        path + "/testVISIO.vsd",  "application/x-tika-msoffice", "application/vnd.visio",
        path + "/testWAR.war",  "application/zip", "application/x-tika-java-web-archive",
        path + "/testWindows-x86-32.exe",  "application/x-msdownload; format=pe32", "application/x-msdownload; format=pe32",
        path + "/testWINMAIL.dat",  "application/vnd.ms-tnef", "application/vnd.ms-tnef",
        path + "/testWMF.wmf",  "application/x-msmetafile", "application/x-msmetafile",
    };
    
    for (int i = 0; i < files.length; i += 3) {
      byte[] body = Files.toByteArray(new File(files[i+0]));
      ListMultimap<String, Object> emptyMap = ArrayListMultimap.create();
      Record event = createEvent(new ByteArrayInputStream(body), emptyMap);
      assertEquals(files[i+1], detect(event, false));
    }
    
    for (int i = 0; i < files.length; i += 3) {
      byte[] body = Files.toByteArray(new File(files[i+0]));
      ListMultimap headers = ImmutableListMultimap.of(Fields.ATTACHMENT_NAME, new File(files[i+0]).getName());
      Record event = createEvent(new ByteArrayInputStream(body), headers);
      assertEquals(files[i+2], detect(event, true));
    }
    
    for (int i = 0; i < files.length; i += 3) {
      byte[] body = Files.toByteArray(new File(files[i+0]));
      ListMultimap headers = ImmutableListMultimap.of(Fields.ATTACHMENT_NAME, new File(files[i+0]).getPath());
      Record event = createEvent(new ByteArrayInputStream(body), headers);
      assertEquals(files[i+2], detect(event, true));
    }

    // test excludeParameters flag:
    boolean excludeParameters = true;
    byte[] body = Files.toByteArray(new File(path + "/testWindows-x86-32.exe"));
    ListMultimap headers = ImmutableListMultimap.of(Fields.ATTACHMENT_NAME, new File(path + "/testWindows-x86-32.exe").getPath());
    Record event = createEvent(new ByteArrayInputStream(body), headers);
    assertEquals("application/x-msdownload", detect(event, true, excludeParameters));
  }

  private Record createEvent(byte[] bytes) {
    ListMultimap<String, Object> emptyMap = ArrayListMultimap.create();
    return createEvent(new ByteArrayInputStream(bytes), emptyMap);
  }
  
  private Record createEvent(InputStream in, ListMultimap<String, Object> headers) {
    Record record = new Record();
    record.getFields().putAll(headers);
    record.replaceValues(Fields.ATTACHMENT_BODY, in);
    return record;
  }
  
  private String detect(Record event, boolean includeMetaData) throws IOException {
    return detect(event, includeMetaData, false);
  }
  
  private String detect(Record event, boolean includeMetaData, boolean excludeParameters) throws IOException {
    List key = Arrays.asList(includeMetaData, excludeParameters);
    Command cachedMorphline = morphlineCache.get(key);
    if (cachedMorphline == null) { // avoid recompiling time and again (performance)
      Config override = ConfigFactory.parseString("INCLUDE_META_DATA : " + includeMetaData + "\nEXCLUDE_PARAMETERS : " + excludeParameters);
      cachedMorphline = createMorphline("test-morphlines/detectMimeTypesWithDefaultMimeTypesAndFile", override);
      morphlineCache.put(key, cachedMorphline);
    }
    collector.reset();
    assertTrue(cachedMorphline.process(event));
    String mimeType = (String) collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_MIME_TYPE);
    return mimeType;
  }
  
}
