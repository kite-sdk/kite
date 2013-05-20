/**
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
package com.cloudera.cdk.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Closeables;
import com.google.common.io.Files;

/**
 * 
 * This class is an helper to copy the jars needed by the job in the Distributed cache.
 * 
 * It speeds up the start of the job and ease the share of libs between jobs.
 * 
 * It creates a .md5 checksum file in HDFS in the directory where the libs are. The checksum is verified before copying
 * the library to HDFS.
 * 
 * In order to find the jar or creating the job's Jar It use a modified version of org.apache.hadoop.util.JarFinder that
 * is found in Hadoop 0.23
 * 
 * 
 * @author tbussier (tony.bussieres@ticksmith.com)
 * 
 * 
 * 
 */
public class JobClasspathHelper {

	private static final Logger logger = LoggerFactory.getLogger(JobClasspathHelper.class);

	
	
	public void prepareClasspath(final Configuration conf,final Path libDir, Class<?>... classesToInclude) throws Exception {
		FileSystem fs = null;
		try {
			List<Class<?>> classList = new ArrayList<Class<?>>(Arrays.asList(classesToInclude));
			fs = FileSystem.get(conf);
			Map<String, String> jarList = new TreeMap<String, String>();
			for (Class<?> clz : classList) {
				if (clz != null) {
					String jar = JarFinder.getJar(clz);
					if (!jarList.containsKey(jar)) {
						HashCode hc = Files.hash(new File(jar), Hashing.md5());
						String md5sum = hc.toString();
						//We should not push core Hadoop classes with this tool.
						//Should it be the responsibility of the developer or we let
						//this fence here
						if (!clz.getName().startsWith("org.apache.hadoop.io.")
								&& !clz.getName().startsWith("org.apache.hadoop.mapred")) {
							jarList.put(jar, md5sum);
						} else {
							logger.info("Ignoring {}, since it looks like it's from Hadoop's core libs",jar);
						}
					}
				}
			}
			for (Entry<String, String> entry : jarList.entrySet()) {
				String jar = entry.getKey();
				String jarFilename = new File(jar).getName();
				String md5sum = entry.getValue();
				logger.info("Jar {}. MD5 : [{}]", jar, md5sum);
				Path remote = new Path(libDir, jarFilename);
				Path remoteMd5Path = new Path(libDir, jarFilename + ".md5");
				if (!fs.exists(remoteMd5Path)) {
					createMd5SumFile(fs, md5sum, remoteMd5Path);
				}
				if (!fs.exists(remote)) {
					copyJarToHDFS(fs, jar, jarFilename, md5sum, remote, remoteMd5Path);
				} else {
					String oldMd5sum = new BufferedReader(new InputStreamReader(fs.open(remoteMd5Path))).readLine();
					if (md5sum.equals(oldMd5sum)) {
						logger.info("Jar {} already exists [{}] and md5sum are equals", jarFilename, remote.toUri()
								.toASCIIString());
					} else {
						logger.info("Jar {} already exists [{}] and md5sum are different!", jarFilename, remote.toUri()
								.toASCIIString());
						copyJarToHDFS(fs, jar, jarFilename, md5sum, remote, remoteMd5Path);
					}
				}
				DistributedCache.addFileToClassPath(remote, conf, fs);
			}
			DistributedCache.createSymlink(conf);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	private void copyJarToHDFS(FileSystem fs, String jar, String jarFilename, String md5sum, Path remote,
			Path remoteMd5Path) throws IOException {
		logger.info("Copying {} to [{}]", jarFilename, remote.toUri().toASCIIString());
		fs.copyFromLocalFile(new Path(jar), remote);

		// these tmp files are created by JarFinder, we need to clean them after.
		createMd5SumFile(fs, md5sum, remoteMd5Path);
		if (remote.getName().startsWith(JarFinder.TMP_HADOOP)) {
			fs.deleteOnExit(remote);
		}
		if (remoteMd5Path.getName().startsWith(JarFinder.TMP_HADOOP)) {
			fs.deleteOnExit(remoteMd5Path);
		}
	}

	private void createMd5SumFile(FileSystem fs, String md5sum, Path remoteMd5Path) {
		FSDataOutputStream os =null;
		try {
			os = fs.create(remoteMd5Path, true);
			os.writeBytes(md5sum);
			os.flush();
		} catch (Exception e) {
			logger.error("{}", e);
		} finally {
			Closeables.closeQuietly(os);
		}
	};

}
