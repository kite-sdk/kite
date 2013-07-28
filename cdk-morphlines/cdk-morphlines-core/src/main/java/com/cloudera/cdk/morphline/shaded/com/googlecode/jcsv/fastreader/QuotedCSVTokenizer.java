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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.fastreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;


/**
 * This implementation follows the csv formatting standard, described in:
 * http://en.wikipedia.org/wiki/Comma-separated_values
 *
 * If you have a more specific csv format, such as constant column widths or
 * columns that do not need to be quoted, you may consider to write a more simple
 * but performant CSVTokenizer.
 *
 */
public final class QuotedCSVTokenizer implements CSVTokenizer {
	private enum State {
		NORMAL, QUOTED
	}

	@Override
	public void tokenizeLine(String line, CSVStrategy strategy, BufferedReader reader, List<String> tokens) throws IOException {
		final char DELIMITER = strategy.getDelimiter();
		final char QUOTE = strategy.getQuoteCharacter();
		final char NEW_LINE = '\n';

		final StringBuilder sb = new StringBuilder(30);

		line += NEW_LINE;
		State state = State.NORMAL;

		int pointer = 0;
		while (true) {
			final char c = line.charAt(pointer);

			switch (state) {
				case NORMAL:
					if (c == DELIMITER) {
						tokens.add(sb.toString());
						sb.setLength(0);
					} else if (c == NEW_LINE) {
						if (!(tokens.size() == 0 && sb.length() == 0)) {
							tokens.add(sb.toString());
						}
						return;
					} else if (c == QUOTE) {
						if (sb.length() == 0) {
							state = State.QUOTED;
						} else if (line.charAt(pointer + 1) == QUOTE) {
							sb.append(c);
							pointer++;
						} else {
							state = State.QUOTED;
						}
					} else {
						sb.append(c);
					}
					break;

				case QUOTED:
					if (c == NEW_LINE) {
						sb.append(NEW_LINE);
						pointer = -1;
						line = reader.readLine();
						if (line == null) {
							throw new IllegalStateException("unexpected end of file, unclosed quotation");
						}
						line += NEW_LINE;
					} else if (c == QUOTE) {
						if (line.charAt(pointer + 1) == QUOTE) {
							sb.append(c);
							pointer++;
							break;
						} else {
							state = State.NORMAL;
						}
					} else {
						sb.append(c);
					}
					break;
			}

			pointer++;
		}
	}
}
