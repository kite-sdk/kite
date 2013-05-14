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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.CSVStrategy;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CSVEntryFilter;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CSVEntryParser;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CSVReader;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CSVTokenizer;

public class CSVReaderImpl<E> implements CSVReader<E> {
	private final BufferedReader reader;
	private final CSVStrategy strategy;
	private final CSVEntryParser<E> entryParser;
	private final CSVEntryFilter<E> entryFilter;
	private final CSVTokenizer tokenizer;

	private boolean firstLineRead = false;

	CSVReaderImpl(CSVReaderBuilder<E> builder) {
		this.reader = new BufferedReader(builder.reader);
		this.strategy = builder.strategy;
		this.entryParser = builder.entryParser;
		this.entryFilter = builder.entryFilter;
		this.tokenizer = builder.tokenizer;
	}


	@Override
	public List<E> readAll() throws IOException {
		List<E> entries = new ArrayList<E>();

		E entry = null;
		while ((entry = readNext()) != null) {
			entries.add(entry);
		}

		return entries;
	}

	@Override
	public E readNext() throws IOException {
		if (strategy.isSkipHeader() && !firstLineRead) {
			reader.readLine();
		}

		E entry = null;
		boolean validEntry = false;
		do {
			String line = readLine();
			if (line == null) {
				return null;
			}

			if (line.trim().length() == 0 && strategy.isIgnoreEmptyLines()) {
				continue;
			}

			if (isCommentLine(line)) {
				continue;
			}

			List<String> data = tokenizer.tokenizeLine(line, strategy, reader);
			entry = entryParser.parseEntry(data.toArray(new String[data.size()]));

			validEntry = entryFilter != null ? entryFilter.match(entry) : true;
		} while (!validEntry);

		firstLineRead = true;

		return entry;
	}

	@Override
	public List<String> readHeader() throws IOException {
		if (firstLineRead) {
			throw new IllegalStateException("can not read header, readHeader() must be the first call on this reader");
		}

		String line = readLine();
		if (line == null) {
			throw new IllegalStateException("reached EOF while reading the header");
		}

		List<String> header = tokenizer.tokenizeLine(line, strategy, reader);
		return header;
	}

	/**
	 * Returns the Iterator for this CSVReaderImpl.
	 *
	 * @return Iterator<E> the iterator
	 */
	@Override
	public Iterator<E> iterator() {
		return new CSVIterator();
	}

	/**
	 * {@link java.io.Closeable#close()}
	 */
	@Override
	public void close() throws IOException {
		reader.close();
	}

	private boolean isCommentLine(String line) {
		return line.startsWith(String.valueOf(strategy.getCommentIndicator()));
	}

	/**
	 * Reads a line from the given reader and sets the firstLineRead flag.
	 *
	 * @return the read line
	 * @throws IOException
	 */
	private String readLine() throws IOException {
		String line = reader.readLine();
		firstLineRead = true;
		return line;
	}

	private class CSVIterator implements Iterator<E> {
		private E nextEntry;

		@Override
		public boolean hasNext() {
			if (nextEntry != null) {
				return true;
			}

			try {
				nextEntry = readNext();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}

			return nextEntry != null;
		}

		@Override
		public E next() {
			E entry = null;
			if (nextEntry != null) {
				entry = nextEntry;
				nextEntry = null;
			} else {
				try {
					entry = readNext();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}

			return entry;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("this iterator doesn't support object deletion");
		}
	}
}
