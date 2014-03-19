/**
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
package org.kitesdk.data;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * <p>
 * A service provider interface for metadata system plugins.
 * </p>
 * <p>
 * Implementations of {@link DatasetRepository} are written against this
 * interface and, by extension, can store dataset metadata information contained
 * in the {@link DatasetDescriptor} in a system of the user's choosing.
 * </p>
 * <p>
 * Implementations of {@link MetadataProvider} are typically not thread-safe; that is,
 * the behavior when accessing a single instance from multiple threads is undefined.
 * </p>
 *
 * @deprecated will be moved in 0.14.0; moved to service provider interface.
 */
@Deprecated
@NotThreadSafe
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="NM_SAME_SIMPLE_NAME_AS_INTERFACE",
    justification="Replaced by parent interface")
public interface MetadataProvider extends org.kitesdk.data.spi.MetadataProvider {
}
