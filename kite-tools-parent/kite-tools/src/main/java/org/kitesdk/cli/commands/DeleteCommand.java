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
package org.kitesdk.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.PartitionView;
import org.kitesdk.data.View;
import org.slf4j.Logger;

@Parameters(commandDescription = "Delete a view or a dataset and its metadata")
public class DeleteCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset or view>")
  List<String> targets;

  @Parameter(names="--dry-run",
      description="Displays the view or dataset uris that would be deleted but does not perform actual deletion.")
  boolean dryRun=false;

  public DeleteCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    if (targets == null || targets.isEmpty()) {
      throw new IllegalArgumentException("No views or datasets were specified.");
    }

    for (String uriOrName : targets) {
      if (isViewUri(uriOrName)) {
        View view = Datasets.load(uriOrName);
        Preconditions.checkArgument(viewMatches(view.getUri(), uriOrName),
            "Resolved view does not match requested view: " + view.getUri());

        if(console.isDebugEnabled()){
          for(PartitionView<?> pv: (Iterable<PartitionView<?>>) view.getCoveringPartitions()){
            if(pv.canDelete()) {
              console.debug("Deleting partition {}", pv.getLocation());
            }else{
              console.debug("Deleting is not supported for partition {}", pv.getLocation());
            }
          }
        }

        if(!dryRun) {
          view.deleteAll();
        }
      } else if (isDatasetUri(uriOrName)) {
        if (!dryRun){
          Datasets.delete(uriOrName);
        }
      } else {
        if(!dryRun) {
          getDatasetRepository().delete(namespace, uriOrName);
        }
      }
      console.debug(dryRun ? "Would have deleted {}" : "Deleted {}", uriOrName);
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Delete all data and metadata for the dataset \"users\":",
        "users",
        "# Delete all records in a view of \"events\":",
        "view:hive:events?source=m1",
        "# Delete all data and metadata for a dataset by URI:",
        "dataset:hbase:zk1,zk2/users"
    );
  }
}
