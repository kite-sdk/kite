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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal;

import java.io.Reader;

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.CSVStrategy;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CSVEntryFilter;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CSVEntryParser;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CSVReader;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CSVTokenizer;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CachedCSVReader;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.util.Builder;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.CSVReaderBuilder;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.CSVReaderImpl;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.CSVTokenizerImpl;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.CachedCSVReaderImpl;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.DefaultCSVEntryParser;

/**
 * The Builder that creates the CSVReaderImpl objects.
 *
 * @param <E>
 *            The Type that your rows represent
 */
public class CSVReaderBuilder<E> implements Builder<CSVReader<E>> {

	final Reader reader;
	CSVEntryParser<E> entryParser;
	CSVStrategy strategy = CSVStrategy.DEFAULT;
	CSVEntryFilter<E> entryFilter;
	CSVTokenizer tokenizer = new CSVTokenizerImpl();

	/**
	 * @param reader
	 *            the csv reader
	 */
	public CSVReaderBuilder(Reader reader) {
		this.reader = reader;
	}

	/**
	 * Sets the strategy that the CSVReaderImpl will use. If you don't specify a
	 * csv strategy, the default csv strategy <code>CSVStrategy.DEFAULT</code>
	 * will be used.
	 *
	 * @param strategy
	 *            the csv strategy
	 * @return this builder
	 */
	public CSVReaderBuilder<E> strategy(CSVStrategy strategy) {
		this.strategy = strategy;
		return this;
	}

	/**
	 * Sets the entry parser that the CSVReaderImpl will use.
	 *
	 * @param entryParser
	 *            the entry parser
	 * @return this builder
	 */
	public CSVReaderBuilder<E> entryParser(CSVEntryParser<E> entryParser) {
		this.entryParser = entryParser;
		return this;
	}

	/**
	 * Sets the entry filter that the CSVReaderImpl will use.
	 *
	 * @param entryFilter
	 *            the entry filter
	 * @return this builder
	 */
	public CSVReaderBuilder<E> entryFilter(CSVEntryFilter<E> entryFilter) {
		this.entryFilter = entryFilter;
		return this;
	}

	/**
	 * Sets the csv tokenizer implementation. If you don't specify your own csv
	 * tokenizer strategy, the default tokenizer will be used.
	 * {@link com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.CSVTokenizerImpl}
	 *
	 * @param tokenizer
	 *            the csv tokenizer
	 * @return this builder
	 */
	public CSVReaderBuilder<E> tokenizer(CSVTokenizer tokenizer) {
		this.tokenizer = tokenizer;
		return this;
	}

	/**
	 * This method finally creates the CSVReaderImpl using the specified
	 * configuration.
	 *
	 * @return the CSVReaderImpl instance
	 */
	@Override
	public CSVReader<E> build() {
		if (entryParser == null) {
			throw new IllegalStateException("you have to specify a csv entry parser");
		}

		return new CSVReaderImpl<E>(this);
	}

	/**
	 * Returns a default configured CSVReaderImpl<String[]>. It uses the
	 * DefaultCSVEntryParser that allows you to convert a csv file into a
	 * List<String[]>.
	 *
	 * @param reader
	 *            the csv reader
	 * @return the CSVReaderImpl
	 */
	public static CSVReader<String[]> newDefaultReader(Reader reader) {
		return new CSVReaderBuilder<String[]>(reader).entryParser(new DefaultCSVEntryParser()).build();
	}

	/**
	 * Creates a cached csv reader, based on the csv reader.
	 *
	 * @param reader
	 *            the csv reader
	 * @return a cached csv reader
	 */
	public static <E> CachedCSVReader<E> cached(CSVReader<E> reader) {
		return new CachedCSVReaderImpl<E>(reader);
	}
}
