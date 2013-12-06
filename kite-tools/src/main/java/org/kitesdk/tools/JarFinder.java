/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.kitesdk.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.common.base.Preconditions;

/**
 * Finds the Jar for a class. If the class is in a directory in the classpath, it creates a Jar on the fly with the
 * contents of the directory and returns the path to that Jar. If a Jar is created, it is created in the system
 * temporary directory.
 * 
 * 
 * The code of this class originates from org.apache.util.JarFinder
 * 
 * The difference between the original and this one is that when we are looking for a jar with a class that is not from an
 * external jar (for example the job itself), JarFinder creates the jar (as it is explained in the javadoc)
 * 
 * There was a need to differenced the jars that were created at runtime with the external ones. The TMP_HADOOP constant
 * is prepended to the created jar.
 * 
 * The visibility of the class as been changed to default.
 * 
 */
class JarFinder {

  static final String TMP_HADOOP = "tmp__hadoop-";

  private static void zipDir(File dir, String relativePath, ZipOutputStream zos) throws IOException {
    Preconditions.checkNotNull(relativePath, "relativePath");
    Preconditions.checkNotNull(zos, "zos");
    zipDir(dir, relativePath, zos, true);
    zos.close();
  }

  private static void zipDir(File dir, String relativePath, ZipOutputStream zos, boolean start) throws IOException {
    String[] dirList = dir.list();
    for (String aDirList : dirList) {
      File f = new File(dir, aDirList);
      if (!f.isHidden()) {
        if (f.isDirectory()) {
          if (!start) {
            ZipEntry dirEntry = new ZipEntry(relativePath + f.getName() + "/");
            zos.putNextEntry(dirEntry);
            zos.write(new byte[] {}); // to make find bugs happy.
            zos.closeEntry();
          }
          String filePath = f.getPath();
          File file = new File(filePath);
          zipDir(file, relativePath + f.getName() + "/", zos, false);
        } else {
          ZipEntry anEntry = new ZipEntry(relativePath + f.getName());
          zos.putNextEntry(anEntry);
          InputStream is = null;
          try {
            is = new FileInputStream(f);
            byte[] arr = new byte[4096];
            int read = is.read(arr);
            while (read > -1) {
              zos.write(arr, 0, read);
              read = is.read(arr);
            }

          } finally {
            if (is != null) {
              is.close();
            }
          }
          zos.closeEntry();
        }
      }
    }
  }

  private static void createJar(File dir, File jarFile) throws IOException {
    Preconditions.checkNotNull(dir, "dir");
    Preconditions.checkNotNull(jarFile, "jarFile");
    File jarDir = jarFile.getParentFile();
    if (!jarDir.exists()) {
      if (!jarDir.mkdirs()) {
        throw new IOException(MessageFormat.format("could not create dir [{0}]", jarDir));
      }
    }
    JarOutputStream zos = new JarOutputStream(new FileOutputStream(jarFile));
    zipDir(dir, "", zos);
  }

  /**
   * Returns the full path to the Jar containing the class. It always return a JAR.
   * 
   * @param klass
   *            class.
   * 
   * @return path to the Jar containing the class.
   */
  public static String getJar(Class<?> klass) {
    Preconditions.checkNotNull(klass, "klass");
    ClassLoader loader = klass.getClassLoader();
    if (loader != null) {
      String class_file = klass.getName().replaceAll("\\.", "/") + ".class";
      try {
        for (Enumeration<?> itr = loader.getResources(class_file); itr.hasMoreElements();) {
          URL url = (URL) itr.nextElement();
          String path = url.getPath();
          if (path.startsWith("file:")) {
            path = path.substring("file:".length());
          }
          path = URLDecoder.decode(path, "UTF-8");
          if ("jar".equals(url.getProtocol())) {
            path = URLDecoder.decode(path, "UTF-8");
            return path.replaceAll("!.*$", "");
          } else if ("file".equals(url.getProtocol())) {
            String klassName = klass.getName();
            klassName = klassName.replace(".", "/") + ".class";
            path = path.substring(0, path.length() - klassName.length());
            File baseDir = new File(path);
            File testDir = new File(System.getProperty("test.build.dir", "target/test-dir"));
            testDir = testDir.getAbsoluteFile();
            if (!testDir.exists()) {
              if (!testDir.mkdirs()) {
                throw new IOException("Unable to create directory :"+testDir.toString());
              }
            }
            File tempJar = File.createTempFile(TMP_HADOOP, "", testDir);
            tempJar = new File(tempJar.getAbsolutePath() + ".jar");
            createJar(baseDir, tempJar);
            return tempJar.getAbsolutePath();
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

}
