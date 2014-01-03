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

import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;

/**
 * Tools for notifications on the control plane.
 */
public final class Notifications {
  /**
   * The field in which the notification of the lifecycle is stored.
   * @since 0.10.1
   */
  public static final String LIFE_CYCLE = "lifecycle";

  /**
   * The field in which the notification of the lifecycle is stored.
   * @deprecated (since 0.10.1) Use {@link #LIFE_CYCLE} instead.
   */
  @Deprecated
  public static final String LIFE_CYLCLE = LIFE_CYCLE;
  
  /**
   * Get all lifecycle events from the given record.
   * @return A {@link List} of {@link LifecycleEvent} enumerations representing all lifecycles stored in the given notification.
   */
  public static List getLifecycleEvents(Record notification) {
    return notification.get(LIFE_CYLCLE);
  }
  
  /**
   * Notify the given command that a transaction has begun.
   * @param command The {@link Command} to be notified.
   */
  public static void notifyBeginTransaction(Command command) {
    notify(command, LifecycleEvent.BEGIN_TRANSACTION);
  }
  
  /**
   * Notify the given command that a transaction has been committed.
   * @param command The {@link Command} to be notified.
   */
  public static void notifyCommitTransaction(Command command) {
    notify(command, LifecycleEvent.COMMIT_TRANSACTION);
  }
  
  /**
   * Notify the given command that a transaction has been rolled back.
   * @param command The {@link Command} to be notified.
   */
  public static void notifyRollbackTransaction(Command command) {
    notify(command, LifecycleEvent.ROLLBACK_TRANSACTION);
  }
  
  /**
   * Notify the given command that a shutdown command has been issued.
   * @param command The {@link Command} to be notified.
   */
  public static void notifyShutdown(Command command) {
    notify(command, LifecycleEvent.SHUTDOWN);
  }
  
  /**
   * Notify the given command that a session has started.
   * @param command The {@link Command} to be notified.
   */
  public static void notifyStartSession(Command command) {
    notify(command, LifecycleEvent.START_SESSION);
  }
  
  /**
   * Notify a command that a lifecycle event has occurred.
   * @param command The {@link Command} to be notified.
   * @param event The {@link LifecycleEvent} to be passed down to the given command.
   */
  private static void notify(Command command, LifecycleEvent event) {
    Record notification = new Record();
    notification.put(LIFE_CYCLE, event);
    command.notify(notification);
  }
  
  /**
   * Determine whether or not the given notification contains the given lifecycle event.
   * @param notification A {@link Record} that represents a notification.
   * @param event A {@link LifecycleEvent} enumeration that is to be searched for in the given notification.
   */
  public static boolean containsLifecycleEvent(Record notification, LifecycleEvent event) {
    return getLifecycleEvents(notification).contains(event);
  }
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * Enumerations of the standard lifecycle events.
   */
  public static enum LifecycleEvent {
    /**
     * A transaction has been started.
     */
    BEGIN_TRANSACTION,
    /**
     * A transaction has been commited.
     */
    COMMIT_TRANSACTION,
    /**
     * A transaction has been rolled back.
     */
    ROLLBACK_TRANSACTION,
    /**
     * A shutdown has been initiated.
     */
    SHUTDOWN,
    /**
     * A session has been started.
     */
    START_SESSION;
  }     

}
