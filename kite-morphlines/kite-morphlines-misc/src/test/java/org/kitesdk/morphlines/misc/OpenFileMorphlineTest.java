package org.kitesdk.morphlines.misc;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.kitesdk.morphline.api.Collector;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;


public class OpenFileMorphlineTest {

	private String testDirectory;
	private Collector collector;

	private static final String RESOURCES_DIR = "target/test-classes";

	@Before
	public void setUp() throws IOException {
		testDirectory = Files.createTempDir().getAbsolutePath();
		collector = new Collector();
	}

	@org.junit.Test
	public void testBasic() throws IOException {
		String msg = "hello world";

		// setup: copy a file to HDFS to prepare inputFile
		File inputFile = new File(testDirectory + "foo.txt.gz");

		OutputStream out = new FileOutputStream(inputFile);
		out = new GZIPOutputStream(out);
		IOUtils.copy(new ByteArrayInputStream(msg.getBytes(Charsets.UTF_8)), out);
		out.flush();
		out.close();
		Assert.assertTrue(inputFile.exists());

		Command morphline = createMorphline("test-morphlines/openLocalFile");
		Record record = new Record();
		record.put(Fields.ATTACHMENT_BODY, inputFile.toString());
		Assert.assertTrue(morphline.process(record));
		Record expected = new Record();
		expected.put(Fields.MESSAGE, msg);
		Assert.assertEquals(expected, collector.getFirstRecord());
	}

	private Command createMorphline(String file) {
		return new org.kitesdk.morphline.base.Compiler().compile(new File(RESOURCES_DIR + "/" + file + ".conf"), null, createMorphlineContext(), collector);
	}

	private MorphlineContext createMorphlineContext() {
		return new MorphlineContext.Builder().setMetricRegistry(new MetricRegistry()).build();
	}
}
