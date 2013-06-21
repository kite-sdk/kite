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
import java.io.IOException;
import java.util.List;

/**
 * The CSVReader reads and parses csv data.
 *
 * @param <E> the type of the records.
 */
public interface CSVReader<E> extends Iterable<E>, Closeable {

	/**
	 * Reads to the end of the csv file and returns a List of created objects.
	 * Calls readNext() multiple times until null is returned.
	 *
	 * @return List of E
	 * @throws IOException
	 */
	public List<E> readAll() throws IOException;

	/**
	 * Reads the next record from the csv file and returns it.
	 * If the end of the csv file has been reached, this method returns null.
	 *
	 * @return the next entry E, null if the end of the file has been reached
	 * @throws IOException
	 */
	public E readNext() throws IOException;

	/**
	 * Reads and returns the header of the csv file.
	 * This method must be the first call on this CSVReaderImpl.
	 *
	 * @return The csv header
	 * @throws IOException
	 */
	public List<String> readHeader() throws IOException;
}
