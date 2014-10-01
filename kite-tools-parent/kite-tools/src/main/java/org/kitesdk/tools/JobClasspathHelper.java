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
package org.kitesdk.tools;

import com.google.common.io.Closeables;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * <p>
 * This class is an helper to copy the jars needed by the job in the Distributed cache.
 * </p>
 * 
 * <p>
 * This tool helps to setup the job classpath at runtime. It allows library sharing between job. That result in faster
 * jobs setup (since most of the time the libs are already uploaded in HDFS). Before submitting a job, you use this tool
 * to provide the classes that you use in your job.
 * </p>
 * 
 * <p>
 * The tool will find the jar(s), or will create the jars and upload them to a "library" path in HDFS, and it will
 * create an md5 file along the uploaded jar.
 * </p>
 * 
 * <p>
 * In order to find the jar or creating the job's Jar It use a modified version of org.apache.hadoop.util.JarFinder that
 * is found in Hadoop 0.23
 * </p>
 * 
 * <p>
 * If another job needs the same jar and provide the same "library" path it will discover it and use it, without having
 * to lose the time that the upload of the jar would require.
 * </p>
 * 
 * <p>
 * If the jar does not exist in the "library" path, it will upload it. However, if the jar is already in the "library"
 * path, the tool will compute the md5 of the jar and compare with the one found in HDFS, and if there's a difference,
 * the jar will be uploaded.
 * </p>
 * 
 * <p>
 * If it creates a jar (from the classes of the job itself or from the classes in your workspace for example), it will
 * upload the created jar to the "library" path and clean them after the JVM exits.
 * </p>
 * 
 * <p>
 * Here's an example for a job class TestTool.class that requires HashFunction from Guava.
 * </p>
 * 
 * <pre>
 * {@code
 * new JobClasspathHelper().prepareClasspath(getConf(), new Path("/lib/path"), new Class[] { TestTool.class, HashFunction.class});
 * }
 * </pre>
 * 
 * @author tbussier (tony.bussieres@ticksmith.com)
 * @since 0.3.0
 * 
 * 
 */
public class JobClasspathHelper {

  private static final Logger LOG = LoggerFactory.getLogger(JobClasspathHelper.class);

  /**
   * 
   * @param conf
   *            Configuration object for the Job. Used to get the FileSystem associated with it.
   * @param libDir
   *            Destination directory in the FileSystem (Usually HDFS) where to upload and look for the libs.
   * @param classesToInclude
   *            Classes that are needed by the job. JarFinder will look for the jar containing these classes.
   * @throws Exception
   */
  public void prepareClasspath(final Configuration conf, final Path libDir, Class<?>... classesToInclude)
      throws Exception {
    FileSystem fs = null;
    List<Class<?>> classList = new ArrayList<Class<?>>(Arrays.asList(classesToInclude));
    fs = FileSystem.get(conf);
    Map<String, String> jarMd5Map = new TreeMap<String, String>();
    // for each classes we use JarFinder to locate the jar in the local classpath.
    for (Class<?> clz : classList) {
      if (clz != null) {
        String localJarPath = JarFinder.getJar(clz);
        // we don't want to upload the same jar twice
        if (!jarMd5Map.containsKey(localJarPath)) {
          // We should not push core Hadoop classes with this tool.
          // Should it be the responsibility of the developer or we let
          // this fence here?
          if (!clz.getName().startsWith("org.apache.hadoop.")) {
            // we compute the MD5 sum of the local jar
            InputStream in = new FileInputStream(localJarPath);
            boolean threw = true;
            try {
              String md5sum = DigestUtils.md5Hex(in);
              jarMd5Map.put(localJarPath, md5sum);
              threw = false;
            } finally {
              Closeables.close(in, threw);
            }
          } else {
            LOG.info("Ignoring {}, since it looks like it's from Hadoop's core libs", localJarPath);
          }
        }
      }
    }

    for (Entry<String, String> entry : jarMd5Map.entrySet()) {
      Path localJarPath = new Path(entry.getKey());
      String jarFilename = localJarPath.getName();
      String localMd5sum = entry.getValue();
      LOG.info("Jar {}. MD5 : [{}]", localJarPath, localMd5sum);

      Path remoteJarPath = new Path(libDir, jarFilename);
      Path remoteMd5Path = new Path(libDir, jarFilename + ".md5");

      // If the jar file does not exist in HDFS or if the MD5 file does not exist in HDFS,
      // we force the upload of the jar.
      if (!fs.exists(remoteJarPath) || !fs.exists(remoteMd5Path)) {
        copyJarToHDFS(fs, localJarPath, localMd5sum, remoteJarPath, remoteMd5Path);
      } else {
        // If the jar exist,we validate the MD5 file.
        // If the MD5 sum is different, we upload the jar
        FSDataInputStream md5FileStream = null;

        String remoteMd5sum = "";
        try {
          md5FileStream = fs.open(remoteMd5Path);
          byte[] md5bytes = new byte[32];
          if (32 == md5FileStream.read(md5bytes)) {
            remoteMd5sum = new String(md5bytes, Charsets.UTF_8);
          }
        } finally {
          if (md5FileStream != null) {
            md5FileStream.close();
          }
        }

        if (localMd5sum.equals(remoteMd5sum)) {
          LOG.info("Jar {} already exists [{}] and md5sum are equals", jarFilename, remoteJarPath.toUri()
              .toASCIIString());
        } else {
          LOG.info("Jar {} already exists [{}] and md5sum are different!", jarFilename, remoteJarPath
              .toUri().toASCIIString());
          copyJarToHDFS(fs, localJarPath, localMd5sum, remoteJarPath, remoteMd5Path);
        }

      }
      // In all case we want to add the jar to the DistributedCache's classpath
      DistributedCache.addFileToClassPath(remoteJarPath, conf, fs);
    }
    // and we create the symlink (was necessary in earlier versions of Hadoop)
    DistributedCache.createSymlink(conf);
  }

  /**
   * @param fs
   *            File system where to upload the jar.
   * @param localJarPath
   *            The local path where we find the jar.
   * @param md5sum
   *            The MD5 sum of the local jar.
   * @param remoteJarPath
   *            The remote path where to upload the jar.
   * @param remoteMd5Path
   *            The remote path where to create the MD5 file.
   * 
   * @throws IOException
   */
  private void copyJarToHDFS(FileSystem fs, Path localJarPath, String md5sum, Path remoteJarPath, Path remoteMd5Path)
      throws IOException {

    LOG.info("Copying {} to {}", localJarPath.toUri().toASCIIString(), remoteJarPath.toUri().toASCIIString());
    fs.copyFromLocalFile(localJarPath, remoteJarPath);
    // create the MD5 file for this jar.
    createMd5SumFile(fs, md5sum, remoteMd5Path);

    // we need to clean the tmp files that are are created by JarFinder after the JVM exits.
    if (remoteJarPath.getName().startsWith(JarFinder.TMP_HADOOP)) {
      fs.deleteOnExit(remoteJarPath);
    }
    // same for the MD5 file.
    if (remoteMd5Path.getName().startsWith(JarFinder.TMP_HADOOP)) {
      fs.deleteOnExit(remoteMd5Path);
    }
  }

  /**
   * This method creates an file that contains a line with a MD5 sum
   * 
   * @param fs
   *            FileSystem where to create the file.
   * @param md5sum
   *            The string containing the MD5 sum.
   * @param remoteMd5Path
   *            The path where to save the file.
   * @throws IOException
   */
  private void createMd5SumFile(FileSystem fs, String md5sum, Path remoteMd5Path) throws IOException {
    FSDataOutputStream os = null;
    try {
      os = fs.create(remoteMd5Path, true);
      os.writeBytes(md5sum);
      os.flush();
    } catch (Exception e) {
      LOG.error("{}", e);
    } finally {
      if (os != null) {
        os.close();
      }
    }
  };

}
