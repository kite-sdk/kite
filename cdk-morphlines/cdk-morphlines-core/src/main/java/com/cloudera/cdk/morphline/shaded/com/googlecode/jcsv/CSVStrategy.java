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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv;

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.CSVStrategy;

public class CSVStrategy {

	/**
	 * The default CSV strategy.
	 * - delimiter ;
	 * - quote character "
	 * - comment indicator #
	 * - do not skip header
	 * - ignore empty lines 
	 */
	public static final CSVStrategy DEFAULT = new CSVStrategy(';', '"', "#", false, true);

	/**
	 * The USA/UK csv standard.
	 * - delimiter ,
	 * - quote character "
	 * - comment indicator #
	 * - do not skip header
	 * - ignore empty lines
	 */
	public static final CSVStrategy UK_DEFAULT = new CSVStrategy(',', '"', "#", false, true);

	private final char delimiter;
	private final char quoteCharacter;
	private final String commentIndicator;
	private final boolean skipHeader;
	private final boolean ignoreEmptyLines;

	/**
	 * Creates a CSVStrategy.
	 *
	 * @param delimiter
	 * @param quoteCharacter
	 * @param commentIndicator
	 * @param skipHeader
	 * @param ignoreEmptyLines
	 */
	public CSVStrategy(char delimiter, char quoteCharacter, String commentIndicator, boolean skipHeader,
			boolean ignoreEmptyLines) {
		this.delimiter = delimiter;
		this.quoteCharacter = quoteCharacter;
		this.commentIndicator = commentIndicator;
		this.skipHeader = skipHeader;
		this.ignoreEmptyLines = ignoreEmptyLines;
	}

	/**
	 * Returns the delimiter character.
	 */
	public char getDelimiter() {
		return delimiter;
	}

	/**
	 * Returns the quote character.
	 */
	public char getQuoteCharacter() {
		return quoteCharacter;
	}

	/**
	 * Returns the comment indicator.
	 */
	public String getCommentIndicator() {
		return commentIndicator;
	}

	/**
	 * Skip the header?
	 * @return true, if the csv header should be skipped.
	 */
	public boolean isSkipHeader() {
		return skipHeader;
	}

	/**
	 * Ignore empty lines?
	 * @return true, if empty lines should be ignored.
	 */
	public boolean isIgnoreEmptyLines() {
		return ignoreEmptyLines;
	}
}
