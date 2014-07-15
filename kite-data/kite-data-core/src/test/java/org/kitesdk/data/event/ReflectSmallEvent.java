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

public class ReflectSmallEvent {

  /**
   * A unique identifier for the user. Required.
   */
  private long user_id;
  /**
   * A unique identifier for the session. Required.
   */
  private String session_id;

  public ReflectSmallEvent() {
  }

  public ReflectSmallEvent(StandardEvent event) {
    setSession_id(event.getSessionId());
    setUser_id(event.getUserId());
  }

  public String getSession_id() {
    return session_id;
  }

  public final void setSession_id(String session_id) {
    this.session_id = session_id;
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
    final ReflectSmallEvent other = (ReflectSmallEvent) obj;
    return Objects.equal(this.getSession_id(), other.getSession_id()) && Objects.equal(this.getUser_id(), other.getUser_id());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getSession_id(), getUser_id());
  }

}
