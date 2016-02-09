/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.filesystem;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.StorageKey;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * An implementation of {@link PathConversion} that builds a key from the start of the path to the end
 * instead of started at the end and building backwards.  The benefit of implementation is that the
 * partial keys can be created without traversing the entire tree.
 */
public class PartialPathConversion extends PathConversion{

  private final Path rootPath;

  /**
   * Creates a conversion for any path prefixed with the {@code rootDirectory} and partitioned
   * @param rootDirectory the root location from which the path is being evaluated
   * @param schema The schema for the payload dataset
     */
  public PartialPathConversion(Path rootDirectory, Schema schema){
    super(schema);
    this.rootPath = rootDirectory;
  }

  //Supposed to build keys from start to finish vs end to start
  public StorageKey toKey(Path fromPath, StorageKey storage) {
    final List<FieldPartitioner> partitioners =
            Accessor.getDefault().getFieldPartitioners(storage.getPartitionStrategy());

    //Strip off the root directory to get partition segments
    String truncatedPath = fromPath.toString();
    if(truncatedPath.startsWith(rootPath.toString())){
      truncatedPath = truncatedPath.substring(rootPath.toString().length());
    }

    List<String> pathParts = new LinkedList<String>();
    //Check that there are segments to parse.
    if(!truncatedPath.isEmpty()) {
      Path currentPath = new Path(truncatedPath);
      while (currentPath != null) {
        String name = currentPath.getName();
        if(!name.isEmpty()) {
          pathParts.add(currentPath.getName());
        }
        currentPath = currentPath.getParent();
      }

      //list is now last -> first so reverse the list to be first -> last
      Collections.reverse(pathParts);
    }


    final List<Object> values = Lists.newArrayList(
            new Object[pathParts.size()]);
    //for each segment we have get the value for the key
    for(int  i = 0; i < pathParts.size(); i++){
      values.set(i, valueForDirname(
              (FieldPartitioner<?, ?>) partitioners.get(i),
              pathParts.get(i)));
    }

    storage.replaceValues(values);
    return storage;
  }
}
