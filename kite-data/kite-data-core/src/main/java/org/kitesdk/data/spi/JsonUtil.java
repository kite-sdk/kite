/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.ValidationException;

public class JsonUtil {

  public static JsonNode parse(String json) {
    return parse(json, JsonNode.class);
  }

  public static <T> T parse(String json, Class<T> returnType) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json, returnType);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  public static JsonNode parse(File file) {
    return parse(file, JsonNode.class);
  }

  public static <T> T parse(File file, Class<T> returnType) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(file, returnType);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  public static JsonNode parse(InputStream in) {
    return parse(in, JsonNode.class);
  }

  public static <T> T parse(InputStream in, Class<T> returnType) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(in, returnType);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

}
