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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DatasetRepository;
import org.slf4j.Logger;

@Parameters(commandDescription = "Delete a view or a dataset and its metadata")
public class DeleteCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset or view>")
  List<String> targets;

  public DeleteCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    DatasetRepository repo = getDatasetRepository();

    if (targets == null || targets.isEmpty()) {
      throw new IllegalArgumentException("No views or datasets were specified.");
    }

    for (String uriOrName : targets) {
      if (isViewUri(uriOrName)) {
        View view = Datasets.load(uriOrName);
        Preconditions.checkArgument(viewMatches(view.getUri(), uriOrName),
            "Resolved view does not match requested view: " + view.getUri());
        view.deleteAll();
      } else if (isDatasetUri(uriOrName)) {
        Datasets.delete(uriOrName);
      } else {
        repo.delete(namespace, uriOrName);
      }
      console.debug("Deleted {}", uriOrName);
    }

    return 0;
  }

  /**
   * Verify that a view matches the URI that loaded it without extra options.
   * <p>
   * This is used to prevent mis-interpreted URIs from succeeding. For example,
   * the URI: view:file:./table?year=2014&month=3&dy=14 resolves a view for
   * all of March 2014 because "dy" wasn't recognized as "day" and was ignored.
   * This works by verifying that all of the options are accounted for in the
   * final view and would fail the above because "dy" is not in the view's
   * options.
   *
   * @param view a View's URI
   * @param requested the requested View URI
   * @return true if the view's URI and the requested URI match exactly
   */
  @VisibleForTesting
  static boolean viewMatches(URI view, String requested) {
    // test that the requested options are a subset of the final options
    Map<String, String> requestedOptions = optionsForUri(URI.create(requested));
    Map<String, String> finalOptions = optionsForUri(view);
    for (Map.Entry<String, String> entry : requestedOptions.entrySet()) {
      if (!finalOptions.containsKey(entry.getKey()) ||
          !finalOptions.get(entry.getKey()).equals(entry.getValue())) {
        return false;
      }
    }
    return true;
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
