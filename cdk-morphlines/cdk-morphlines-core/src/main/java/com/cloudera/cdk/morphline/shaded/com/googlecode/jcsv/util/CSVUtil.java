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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides some useful functions.
 */
public class CSVUtil {
	private CSVUtil() {
		// Prevent instantiating and inheriting
	}

	/**
	 * Concats the String[] array data to a single String, using the specified
	 * delimiter as the glue.
	 *
	 * <code>implode({"A", "B", "C"}, ";")</code> would result in A;B;C
	 *
	 * @param data
	 *            the strings that should be concatinated
	 * @param delimiter
	 *            the delimiter
	 * @return the concatinated string
	 */
	public static String implode(String[] data, String delimiter) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < data.length; i++) {
			if (i != 0) {
				sb.append(delimiter);
			}

			sb.append(data[i]);
		}

		return sb.toString();
	}

	/**
	 * Splits the provided text into an array, separator specified, preserving
	 * all tokens, including empty tokens created by adjacent separators.
	 *
	 * CSVUtil.split(null, *, true) = null
	 * CSVUtil.split("", *, , true) = []
	 * CSVUtil.split("a.b.c", '.', true) = ["a", "b", "c"]
	 * CSVUtil.split("a...c", '.', true) = ["a", "", "", "c"]
	 * CSVUtil.split("a...c", '.', false) = ["a", "c"]
	 *
	 * @param str
	 *            the string to parse
	 * @param separatorChar
	 *            the seperator char
	 * @param preserveAllTokens
	 *            if true, adjacent separators are treated as empty token
	 *            separators
	 * @return the splitted string
	 */
	public static String[] split(String str, char separatorChar, boolean preserveAllTokens) {
		if (str == null) {
			return null;
		}
		int len = str.length();
		if (len == 0) {
			return new String[0];
		}
		List<String> list = new ArrayList<String>();
		int i = 0, start = 0;
		boolean match = false;
		boolean lastMatch = false;
		while (i < len) {
			if (str.charAt(i) == separatorChar) {
				if (match || preserveAllTokens) {
					list.add(str.substring(start, i));
					match = false;
					lastMatch = true;
				}
				start = ++i;
				continue;
			}
			lastMatch = false;
			match = true;
			i++;
		}
		if (match || preserveAllTokens && lastMatch) {
			list.add(str.substring(start, i));
		}
		return list.toArray(new String[list.size()]);
	}
}
