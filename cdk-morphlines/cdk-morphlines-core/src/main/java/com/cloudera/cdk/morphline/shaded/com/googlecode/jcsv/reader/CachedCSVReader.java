/**
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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader;

import java.io.Closeable;
import java.util.ListIterator;

/**
 * The CacheCSVReader improves the CSVReader with a cache for the read entries.
 * If you need to access the records very often or want to iterate through the
 * list of records back and forth, you might use a cached csv reader.
 *
 * This Interface bundles the methods of the ListIterator and Closeable.
 *
 * @param <E>
 *            the type of the records
 */
public interface CachedCSVReader<E> extends ListIterator<E>, Closeable {
	/**
	 * Returns the i's entry of the csv file. If the entry is already in the
	 * cache, the entry will be returned directly. If not, the reader reads
	 * until the position i is reached or the end of the csv file is reached.
	 *
	 * @param i
	 *            the position
	 * @return the entry at position i
	 * @throws ArrayIndexOutOfBoundsException
	 *             if there is no entry at position i
	 */
	public E get(int i);
}
