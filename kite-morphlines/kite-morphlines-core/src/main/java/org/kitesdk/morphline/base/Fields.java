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
package org.kitesdk.morphline.base;

/**
 * Common record field names.
 */
public final class Fields {

  public static final String ID = "id";
  public static final String BASE_ID = "base_id";
  public static final String TIMESTAMP = "timestamp";
  public static final String MESSAGE = "message"; // the original plain-text message

  public static final String ATTACHMENT_BODY = "_attachment_body";
  public static final String ATTACHMENT_MIME_TYPE = "_attachment_mimetype";
  public static final String ATTACHMENT_CHARSET = "_attachment_charset";
  public static final String ATTACHMENT_NAME = "_attachment_name";

}
