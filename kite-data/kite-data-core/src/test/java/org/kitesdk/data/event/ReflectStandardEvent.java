/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.event;

import com.google.common.base.Objects;

public class ReflectStandardEvent {

  /**
   * Where the event was triggered from in the format
   * {client,server}_{user,app}, e.g. 'client_user'. Required.
   */
  private String event_initiator;
  /**
   * A hierarchical name for the event, with parts separated by ':'. Required.
   */
  private String event_name;
  /**
   * A unique identifier for the user. Required.
   */
  private long user_id;
  /**
   * A unique identifier for the session. Required.
   */
  private String session_id;
  /**
   * The IP address of the host where the event originated. Required.
   */
  private String ip;
  /**
   * The point in time when the event occurred, represented as the number of
   * milliseconds since January 1, 1970, 00:00:00 GMT. Required.
   */
  private long timestamp;

  public ReflectStandardEvent() {
  }

  public ReflectStandardEvent(StandardEvent event) {
    setEvent_initiator(event.getEventInitiator());
    setEvent_name(event.getEventName());
    setIp(event.getIp());
    setSession_id(event.getSessionId());
    setTimestamp(event.getTimestamp());
    setUser_id(event.getUserId());
  }

  public String getEvent_initiator() {
    return event_initiator;
  }

  public final void setEvent_initiator(String event_initiator) {
    this.event_initiator = event_initiator;
  }

  public String getEvent_name() {
    return event_name;
  }

  public final void setEvent_name(String event_name) {
    this.event_name = event_name;
  }

  public String getIp() {
    return ip;
  }

  public final void setIp(String ip) {
    this.ip = ip;
  }

  public String getSession_id() {
    return session_id;
  }

  public final void setSession_id(String session_id) {
    this.session_id = session_id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public final void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getUser_id() {
    return user_id;
  }

  public final void setUser_id(long user_id) {
    this.user_id = user_id;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || !Objects.equal(getClass(), obj.getClass())) {
      return false;
    }
    final ReflectStandardEvent other = (ReflectStandardEvent) obj;
    return Objects.equal(this.getEvent_initiator(), other.getEvent_initiator()) && Objects.equal(this.getEvent_name(), other.getEvent_name()) && Objects.equal(this.getIp(), other.getIp()) && Objects.equal(this.getSession_id(), other.getSession_id()) && Objects.equal(this.getTimestamp(), other.getTimestamp()) && Objects.equal(this.getUser_id(), other.getUser_id());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getEvent_initiator(), getEvent_name(), getIp(), getSession_id(), getTimestamp(), getUser_id());
  }

}
