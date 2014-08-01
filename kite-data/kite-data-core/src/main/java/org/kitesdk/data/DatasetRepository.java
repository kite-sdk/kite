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

import javax.annotation.concurrent.Immutable;

/**
 * <p>
 * A logical repository (storage system) of {@link Dataset}s.
 * </p>
 * <p>
 * Implementations of {@code DatasetRepository} are storage systems that contain
 * zero or more {@link Dataset}s. A repository acts as a factory, as well as a
 * registry, of datasets. You can {@link #create(String, DatasetDescriptor)} a
 * new {@link Dataset} with a name and schema, or retrieve a handle to an
 * existing dataset, by name, by way of the {@link #load(String)} method. While
 * not expressly forbidden, most repositories are expected to support only a
 * single concrete {@link Dataset} implementation.
 * </p>
 * <p>
 * No guarantees are made as to the durability, reliability, or availability of
 * the underlying storage. That is, a {@code DatasetRepository} could be on
 * disk, in memory, or some combination. See the implementation class for
 * details about the guarantees it provides.
 * </p>
 * <p>
 * Implementations of {@link DatasetRepository} are immutable.
 * </p>
 *
 * @see Dataset
 * @see DatasetDescriptor
 *
 * @deprecated will be removed in 0.17.0. Move to using {@link Datasets} instead
 */
@Immutable
@Deprecated
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NM_SAME_SIMPLE_NAME_AS_INTERFACE",
    justification="Part of deprecation process.")
public interface DatasetRepository extends org.kitesdk.data.spi.DatasetRepository {

}
