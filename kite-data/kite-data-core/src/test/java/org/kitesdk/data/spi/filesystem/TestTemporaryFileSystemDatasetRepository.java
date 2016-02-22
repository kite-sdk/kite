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

package org.kitesdk.data.spi.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.TestDatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.MetadataProvider;

public class TestTemporaryFileSystemDatasetRepository extends TestDatasetRepositories {

	public TestTemporaryFileSystemDatasetRepository(boolean distributed) {
		super(distributed);
	}

	public static String KEY = "key";

	@Override
	public DatasetRepository newRepo(MetadataProvider provider) {
		TemporaryFileSystemDatasetRepository sut = new TemporaryFileSystemDatasetRepository(new Configuration(),
				testDirectory, NAMESPACE, KEY);
		this.testProvider = sut.getMetadataProvider();
		return sut;
	}
}
