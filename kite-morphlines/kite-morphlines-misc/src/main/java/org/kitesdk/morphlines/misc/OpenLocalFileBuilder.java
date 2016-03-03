/*
 * Copyright 2013 Cloudera Inc.
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
package org.kitesdk.morphlines.misc;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.zip.GZIPInputStream;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.shaded.com.google.common.io.Closeables;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.typesafe.config.Config;

/**
 * Opens an HDFS file for read and return a corresponding InputStream.
 */
public final class OpenLocalFileBuilder implements CommandBuilder {

	public OpenLocalFileBuilder() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList("openLocalFile");
	}

	@Override
	public Command build(Config config, Command parent, Command child, MorphlineContext context) {
		return new OpenLocalFile(this, config, parent, child, context);
	}

	
	
	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	private static final class OpenLocalFile extends AbstractCommand {
		private String deafultPath = "";

		public OpenLocalFile(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
			super(builder, config, parent, child, context);

			String defaultFileSystemUri = getConfigs().getString(config, "defaultPath", null);
			if (defaultFileSystemUri != null) {
				deafultPath = defaultFileSystemUri;
			}

			validateArguments();
		}

		@Override
		protected boolean doProcess(Record record) {
			for (Object body : record.get(Fields.ATTACHMENT_BODY)) {
				Record outputRecord = record.copy();
				AbstractParser.removeAttachments(outputRecord);
				String pathString = body.toString();
				File file = new File(pathString);

				InputStream in = null;
				try {
					try {
						if (file.isFile() && file.exists()) {

							in = new FileInputStream(file);
							if (pathString.endsWith(".gz")) {
								in = new GZIPInputStream(in, 64 * 1024);
							}
							in = new BufferedInputStream(in);
							outputRecord.put(Fields.ATTACHMENT_BODY, in);
						} else {
							throw new MorphlineRuntimeException("Unable to read File [" + file.getAbsolutePath() + "]");
						}
					} catch (IOException e) {
						throw new MorphlineRuntimeException(e);
					}

					// pass record to next command in chain:
					if (!getChild().process(outputRecord)) {
						return false;
					}
				} finally {
					Closeables.closeQuietly(in);
				}
			}
			return true;
		}

	}
}
