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

/**
 * The CSVEntryParser receives a line of the csv file and converts it
 * to a java object.
 *
 * The default implementation of this interface is
 * {@link com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.DefaultCSVEntryParser}
 * That implementation just returns the String[] array without any conversion.
 *
 * @param <E> The Type that the entry parser creates
 */
public interface CSVEntryParser<E> {
	/**
	 * Converts a row of the csv file to a java object
	 * @param data a row in the csv file
	 * @return the object
	 */
	public E parseEntry(String... data);
}