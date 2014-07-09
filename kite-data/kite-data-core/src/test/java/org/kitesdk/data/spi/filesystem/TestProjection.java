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
package org.kitesdk.data.spi.filesystem;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.net.URI;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.event.IncompatibleEvent;
import org.kitesdk.data.event.SmallEvent;
import org.kitesdk.data.event.StandardEvent;
import org.kitesdk.data.spi.TestRefinableViews;

public class TestProjection extends TestRefinableViews {

  public TestProjection(boolean distributed) {
    super(distributed);
  }

  @Override
  public DatasetRepository newRepo() {
    return new FileSystemDatasetRepository.Builder()
        .configuration(conf)
        .rootDirectory(URI.create("target/data"))
        .build();
  }

  @After
  public void removeDataPath() throws IOException {
    fs.delete(new Path("target/data"), true);
  }

  @Test
  public void testSpecificProjection() throws IOException {
    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = unbounded.newWriter();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }

    Dataset<SmallEvent> dataset = repo.load(unbounded.getDataset().getName(),
        SmallEvent.class);

    Set<SmallEvent> expected = Sets.newHashSet(toSmallEvent(sepEvent),
        toSmallEvent(octEvent), toSmallEvent(novEvent));

    assertContentEquals(expected, dataset);
  }

  @Test
  public void testReflectProjection() throws IOException {
    Dataset<ReflectStandardEvent> original = repo.create("reflectProjection",
        new DatasetDescriptor.Builder()
            .schema(ReflectStandardEvent.class)
            .build(), ReflectStandardEvent.class);

    DatasetWriter<ReflectStandardEvent> writer = null;
    try {
      writer = original.newWriter();
      writer.write(new ReflectStandardEvent(sepEvent));
      writer.write(new ReflectStandardEvent(octEvent));
      writer.write(new ReflectStandardEvent(novEvent));
    } finally {
      Closeables.close(writer, false);
    }

    Dataset<ReflectSmallEvent> dataset = repo.load(original.getName(),
        ReflectSmallEvent.class);

    Set<ReflectSmallEvent> expected = Sets.newHashSet(
        new ReflectSmallEvent(sepEvent), new ReflectSmallEvent(octEvent),
        new ReflectSmallEvent(novEvent));

    assertContentEquals(expected, dataset);
  }

  @Test
  public void testMixedProjection() throws IOException {
    Dataset<StandardEvent> original = repo.create("mixedProjection",
        new DatasetDescriptor.Builder()
            .schema(StandardEvent.class)
            .build(), StandardEvent.class);

    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = original.newWriter();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }

    Dataset<ReflectSmallEvent> dataset = repo.load(original.getName(),
        ReflectSmallEvent.class);

    Set<ReflectSmallEvent> expected = Sets.newHashSet(
        new ReflectSmallEvent(sepEvent), new ReflectSmallEvent(octEvent),
        new ReflectSmallEvent(novEvent));

    assertContentEquals(expected, dataset);
  }

  @Test(expected=IncompatibleSchemaException.class)
  public void testIncompatibleProjection() throws IOException {
    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = unbounded.newWriter();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }

    repo.load(unbounded.getDataset().getName(), IncompatibleEvent.class);
  }

  private static SmallEvent toSmallEvent(StandardEvent event) {
    return SmallEvent.newBuilder()
        .setUserId(event.getUserId())
        .setSessionId(event.getSessionId())
        .build();
  }

  private static IncompatibleEvent toIncompatibleEvent(StandardEvent event) {
    return IncompatibleEvent.newBuilder()
        .setUserId(String.valueOf(event.getUserId()))
        .setSessionId(event.getSessionId())
        .build();
  }

  private static class ReflectStandardEvent {

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
      return Objects.equal(this.getEvent_initiator(), other.getEvent_initiator()) &&
          Objects.equal(this.getEvent_name(), other.getEvent_name()) &&
          Objects.equal(this.getIp(), other.getIp()) &&
          Objects.equal(this.getSession_id(), other.getSession_id()) &&
          Objects.equal(this.getTimestamp(), other.getTimestamp()) &&
          Objects.equal(this.getUser_id(), other.getUser_id());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(getEvent_initiator(), getEvent_name(), getIp(),
          getSession_id(), getTimestamp(), getUser_id());
    }
  }

  private static class ReflectSmallEvent {

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
      return Objects.equal(this.getSession_id(), other.getSession_id()) &&
          Objects.equal(this.getUser_id(), other.getUser_id());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(getSession_id(), getUser_id());
    }
  }
}