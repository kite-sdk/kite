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
package org.kitesdk.morphline.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.typesafe.config.Config;

/**
 * JSON parser that extracts search documents from twitter tweets obtained from the twitter 1% sample firehose with the delimited=length option.
 * For background see https://dev.twitter.com/docs/api/1.1/get/statuses/sample.
 * 
 * The JSON input format is documented at https://dev.twitter.com/docs/platform-objects/tweets
 */
public final class ReadJsonTestTweetsBuilder implements CommandBuilder {

  //public static final String MEDIA_TYPE = "mytwittertest/json+delimited+length";
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readJsonTestTweets");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadJsonTestTweets(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadJsonTestTweets extends AbstractParser {
    
    private final boolean isLengthDelimited;
    private String idPrefix;
    private final ObjectReader reader = new ObjectMapper().reader(JsonNode.class);

    // Fri May 14 02:52:55 +0000 2010
    private SimpleDateFormat formatterFrom = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US);
    private SimpleDateFormat formatterTo = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);

    public ReadJsonTestTweets(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) { 
      super(builder, config, parent, child, context);
      
      this.isLengthDelimited = getConfigs().getBoolean(config, "isLengthDelimited", true);
      this.idPrefix = getConfigs().getString(config, "idPrefix", null);
      if ("random".equals(idPrefix)) {
        idPrefix = String.valueOf(new Random().nextInt());
      } else if (idPrefix == null) {
        idPrefix = "";
      }
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record record, InputStream in) throws IOException {
      String name = (String) record.getFirstValue(Fields.ATTACHMENT_NAME);
      if (name != null && name.endsWith(".gz")) {
        in = new GZIPInputStream(in, 64 * 1024);
      }
      long numRecords = 0;
      BufferedReader bufferedReader = null;
      MappingIterator<JsonNode> iter = null;
      if (isLengthDelimited) {
        bufferedReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));        
      } else {
        iter = reader.readValues(in);
      }
      
      try {
        while (true) {
          JsonNode rootNode;
          if (isLengthDelimited) {
            String json = nextLine(bufferedReader);
            if (json == null) {
              break;
            }
      
            try {
              // src can be a File, URL, InputStream, etc
              rootNode = reader.readValue(json); 
            } catch (JsonParseException e) {
              LOG.info("json parse exception after " + numRecords + " records");
              LOG.debug("json parse exception after " + numRecords + " records", e);
              break;
            }
          } else {
            if (!iter.hasNext()) {
              break;
            }
            rootNode = iter.next();
          }
        
          Record doc = new Record();
          JsonNode user = rootNode.get("user");
          JsonNode idNode = rootNode.get("id_str");
          if (idNode == null || idNode.textValue() == null) {
            continue; // skip
          }
      
          doc.put("id", idPrefix + idNode.textValue());
          tryAddDate(doc, "created_at", rootNode.get("created_at"));          
          tryAddString(doc, "source", rootNode.get("source"));
          tryAddString(doc, "text", rootNode.get("text"));
          tryAddInt(doc, "retweet_count", rootNode.get("retweet_count"));
          tryAddBool(doc, "retweeted", rootNode.get("retweeted"));
          tryAddLong(doc, "in_reply_to_user_id", rootNode.get("in_reply_to_user_id"));
          tryAddLong(doc, "in_reply_to_status_id", rootNode.get("in_reply_to_status_id"));
          tryAddString(doc, "media_url_https", rootNode.get("media_url_https"));
          tryAddString(doc, "expanded_url", rootNode.get("expanded_url"));
      
          tryAddInt(doc, "user_friends_count", user.get("friends_count"));
          tryAddString(doc, "user_location", user.get("location"));
          tryAddString(doc, "user_description", user.get("description"));
          tryAddInt(doc, "user_statuses_count", user.get("statuses_count"));
          tryAddInt(doc, "user_followers_count", user.get("followers_count"));
          tryAddString(doc, "user_screen_name", user.get("screen_name"));
          tryAddString(doc, "user_name", user.get("name"));
          
          incrementNumRecords();
          LOG.debug("tweetdoc: {}", doc);
          if (!getChild().process(doc)) {
            return false;
          }
          numRecords++;
        }
      } finally {
        if (iter != null) {
          iter.close();
        }
        LOG.debug("processed {} records", numRecords);
      }
      return true;
    }
  
    private String nextLine(BufferedReader reader) throws IOException {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.length() > 0)
          break; // ignore empty lines
      }
      if (line == null)
        return null;
      Integer.parseInt(line); // sanity check
  
      while ((line = reader.readLine()) != null) {
        if (line.length() > 0)
          break; // ignore empty lines
      }
      return line;
    }
  
    private void tryAddDate(Record doc, String solr_field, JsonNode node) {
      if (node == null)
        return;
      String val = node.asText();
      if (val == null) {
        return;
      }
      try {
  //      String tmp = formatterTo.format(formatterFrom.parse(val.trim()));
        doc.put(solr_field, formatterTo.format(formatterFrom.parse(val.trim())));
      } catch (Exception e) {
        LOG.error("Could not parse date " + val);
  //      ++exceptionCount;
      }
    }
  
    private void tryAddLong(Record doc, String solr_field, JsonNode node) {
      if (node == null)
        return;
      Long val = node.asLong();
      if (val == null) {
        return;
      }
      doc.put(solr_field, val);
    }
  
    private void tryAddInt(Record doc, String solr_field, JsonNode node) {
      if (node == null)
        return;
      Integer val = node.asInt();
      if (val == null) {
        return;
      }
      doc.put(solr_field, val);
    }
  
    private void tryAddBool(Record doc, String solr_field, JsonNode node) {
      if (node == null)
        return;
      Boolean val = node.asBoolean();
      if (val == null) {
        return;
      }
      doc.put(solr_field, val);
    }
  
    private void tryAddString(Record doc, String solr_field, JsonNode node) {
      if (node == null)
        return;
      String val = node.asText();
      if (val == null) {
        return;
      }
      doc.put(solr_field, val);
    }  
  
  }

}
