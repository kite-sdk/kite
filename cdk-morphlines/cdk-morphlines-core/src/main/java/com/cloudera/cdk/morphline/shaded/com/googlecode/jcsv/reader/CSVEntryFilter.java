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
 * The CSVEntryFilter is used to filter the records of a
 * csv file.
 *
 * @param <E> the type of the records
 */
public interface CSVEntryFilter<E> {

	/**
	 * Checks whether the object e matches this filter.
	 *
	 * @param e The object that is to be tested
	 * @return true, if e matches this filter
	 */
	public boolean match(E e);
}
