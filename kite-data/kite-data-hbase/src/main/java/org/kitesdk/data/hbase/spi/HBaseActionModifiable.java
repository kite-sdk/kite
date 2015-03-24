/**
 * Copyright 2015 Cloudera Inc.
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

package org.kitesdk.data.hbase.spi;

import org.kitesdk.data.hbase.impl.DeleteActionModifier;
import org.kitesdk.data.hbase.impl.GetModifier;
import org.kitesdk.data.hbase.impl.PutActionModifier;
import org.kitesdk.data.hbase.impl.ScanModifier;

import java.util.List;

/**
 * Allows SPI hooks into HBase Actions (GET/SCAN/PUT/DELETE).
 * Any hooks added is persistent on the Dataset Views, Readers and Writers.
 * ActionModifier needs to be removed manually
 */
public interface HBaseActionModifiable {
  /**
   * Register a GetModifier to be called before every Get is executed on HBase.
   * This GetModifier will be replaced if already registered and added if not.
   * Equality is checked by calling the equals() on the getModifier passed.
   * GetModifiers will be called in the order they are added to the
   * template, so if any modifier is destructive, it must be added in the right
   * order.
   *
   * @param getModifier The GetModifier to register.
   */
  void registerGetModifier(GetModifier getModifier);

  /**
   * Returns a list of Get Modifiers currently registered
   *
   * @return List of GetModifier
   */
  List<GetModifier> getGetModifiers();

  /**
   * Register a PutActionModifier to be called before every Put is executed on
   * HBase. This PutActionModifier will be replaced if already registered and
   * added if not. Equality is checked by calling the equals() on the
   * putActionModifier passed. PutActionModifiers will be called in the order
   * they are added, so if any modifier is destructive,
   * it must be added in the right order.
   *
   * @param putActionModifier The PutActionModifier to register.
   */
  void registerPutActionModifier(PutActionModifier putActionModifier);

  /**
   * Returns a list of Put Modifiers currently registered
   *
   * @return List of PutActionModifier
   */
  List<PutActionModifier> getPutActionModifiers();

  /**
   * Register a DeleteActionModifier to be called before every Delete is
   * executed on HBase. This DeleteActionModifier will be replaced if already
   * registered and added if its not. Equality is checked by calling the equals()
   * on the deleteActionModifier passed. DeleteActionModifiers will be called
   * in the order they are added, so if any modifier is destructive,
   * it must be added in the right order.
   *
   * @param deleteActionModifier The DeleteActionModifier to register.
   */
  void registerDeleteModifier(DeleteActionModifier deleteActionModifier);

  /**
   * Returns a list of Delete Modifiers currently registered
   *
   * @return List of DeleteActionModifier
   */
  List<DeleteActionModifier> getDeleteActionModifiers();

  /**
   * Register a ScanModifier to be called before every Scan is executed on
   * HBase. This ScanModifier will be replaced if already registered and added
   * if not. Equality is checked by calling the equals() on the scanModifier passed.
   * ScanModifiers will be called in the order they are added,
   * so if any modifier is destructive, it must be added in the
   * right order.
   *
   * @param scanModifier The ScanModifier to register.
   */
  void registerScanModifier(ScanModifier scanModifier);

  /**
   * Returns a list of Scan Modifiers currently registered
   *
   * @return List of ScanModifier
   */
  List<ScanModifier> getScanModifiers();

  /**
   * Clear all GetModifiers registered with registerGetModifier.
   */
  void clearGetModifiers();

  /**
   * Clear all PutActionModifiers registered with registerPutActionModifier.
   */
  void clearPutActionModifiers();

  /**
   * Clear all DeleteActionModifiers registered with
   * registerDeleteActionModifier.
   */
  void clearDeleteActionModifiers();

  /**
   * Clear all ScanModifiers registered with registerScanModifier.
   */
  void clearScanModifiers();

  /**
   * Clear all modifiers registered with the template for all operations.
   */
  void clearAllModifiers();

}
