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
package org.kitesdk.data.crunch.morphline;


import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;


/**
 * A DoFn that periodically sends progress heartbeats to the job tracker in order to avoid timeouts
 * for long-running tasks.
 */
public final class HeartbeatFn<T> extends DoFn<T,T> {

  private transient HeartBeater heartBeater;

  @Override
  public void initialize() {
    heartBeater = new HeartBeater(getContext());
  }

  @Override
  public void process(T item, Emitter<T> emitter) {
    heartBeater.needHeartBeat();
    try {
      emitter.emit(item);
    } finally {
      heartBeater.cancelHeartBeat();
    }
  }

  @Override
  public void cleanup(Emitter<T> emitter) {
    heartBeater.close();
  }

}
