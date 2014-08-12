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
package org.kitesdk.data.hbase.avro.example;

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Key;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.hbase.HBaseDatasetRepository;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * This is an example that demonstrates basic Kite HBase functionality. It uses a
 * fictional "user profile" use case that needs basic user profile data (first
 * name, last name, etc...) and user action log data (login, profile changed,
 * etc...) persisted in a wide user table.
 * 
 * By using a wide user table, we are able to atomically fetch user profile and
 * action data with a single HBase request, as well as atomically update both
 * profile and action log data.
 * 
 * The basic Kite HBase functionality demonstrated includes basic scanning,
 * putting, composite datasets, and optimistic concurrency control (OCC).
 */
public class UserProfileDatasetExample {

  /**
   * The user profile dataset
   */
  private final RandomAccessDataset<UserProfileModel2> userProfileDataset;

  /**
   * The user actions dataset. User actions are stored in the same table along side
   * the user profiles.
   */
  private final RandomAccessDataset<UserActionsModel2> userActionsDataset;

  /**
   * A composite dataset that encapsulates the two entity types that can be stored
   * in the user_profile table (UserProfileModel and UserActionsModel).
   * UserProfileActionsModel is the composite type this dao returns.
   */
  private final RandomAccessDataset<UserProfileActionsModel2> userProfileActionsDataset;

  /**
   * The constructor will start by registering the schemas with the meta store
   * table in HBase, and create the required tables to run.
   */
  public UserProfileDatasetExample() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);

    // Delete the table if it exists so we start fresh.
    if (admin.tableExists("kite_example_user_profiles")) {
      admin.disableTable("kite_example_user_profiles");
      admin.deleteTable("kite_example_user_profiles");
    }

    HBaseDatasetRepository repo = new HBaseDatasetRepository.Builder()
        .configuration(conf).build();

    // TODO: change to use namespace (CDK-140)

    DatasetDescriptor userProfileDatasetDescriptor =
        new DatasetDescriptor.Builder().schema(UserProfileModel2.SCHEMA$).build();
    userProfileDataset = repo.create("default", "kite_example_user_profiles.UserProfileModel2",
        userProfileDatasetDescriptor);

    DatasetDescriptor userActionsDatasetDescriptor =
        new DatasetDescriptor.Builder().schema(UserActionsModel2.SCHEMA$).build();
    userActionsDataset = repo.create("default", "kite_example_user_profiles.UserActionsModel2",
        userActionsDatasetDescriptor);

    DatasetDescriptor userProfileActionsDatasetDescriptor =
        new DatasetDescriptor.Builder().schema(UserProfileActionsModel2.SCHEMA$).build();
    userProfileActionsDataset = repo.create("default", "kite_example_user_profiles.UserProfileActionsProtocol2",
        userProfileActionsDatasetDescriptor);

  }

  /**
   * Print all user profiles.
   * 
   * This method demonstrates how to open a reader that will read the entire
   * table. It has no start or stop keys specified.
   */
  public void printUserProfies() {
    DatasetReader<UserProfileModel2> reader = userProfileDataset.newReader();
    try {
      for (UserProfileModel2 userProfile : reader) {
        System.out.println(userProfile.toString());
      }
    } finally {
      // readers need to be closed.
      reader.close();
    }
  }

  /**
   * Print the user profiles and actions for all users with the provided last
   * name
   * 
   * This method demonstrates how to open a scanner with a start key. It's using
   * the composite dao, so the records it returns will be a composite of both
   * the profile model and actions model.
   * 
   * @param lastName
   *          The last name of users to scan.
   */
  public void printUserProfileActionsForLastName(String lastName) {
    // TODO: use a reader with a start key
    DatasetReader<UserProfileActionsModel2> reader = userProfileActionsDataset.newReader();
    try {
      for (UserProfileActionsModel2 entity : reader) {
        UserProfileModel2 userProfile = entity.getUserProfileModel();
        if (userProfile.getLastName().equals(lastName)) {
          System.out.println(entity.toString());
        }
      }
    } finally {
      // readers need to be closed.
      reader.close();
    }
  }

  /**
   * Create a fresh new user record.
   * 
   * This method demonstrates creating both a UserProfileModel and a
   * UserActionsModel atomically in a single row. When creating a user profile,
   * we add the "created" action to the actions map. This shows how we can use
   * the CompositeDao to accomplish this.
   * 
   * @param firstName
   *          The first name of the user we are creating
   * @param lastName
   *          The last name of the user we are creating
   * @param married
   *          True if this person is married. Otherwise false.
   */
  public void create(String firstName, String lastName, boolean married) {
    long ts = System.currentTimeMillis();

    UserProfileModel2 profileModel = UserProfileModel2.newBuilder()
        .setFirstName(firstName).setLastName(lastName).setMarried(married)
        .setCreated(ts).build();
    UserActionsModel2 actionsModel = UserActionsModel2.newBuilder()
        .setFirstName(firstName).setLastName(lastName)
        .setActions(new HashMap<String, String>()).build();
    actionsModel.getActions().put("profile_created", Long.toString(ts));

    UserProfileActionsModel2 profileActionsModel = UserProfileActionsModel2
        .newBuilder().setUserProfileModel(profileModel)
        .setUserActionsModel(actionsModel).build();

    if (!userProfileActionsDataset.put(profileActionsModel)) {
      // If put returns false, a user already existed at this row
      System.out
          .println("Creating a new user profile failed due to a write conflict.");
    }
  }

  /**
   * Update the married status of a new user record.
   * 
   * This method demonstrates updating both a UserProfileModel and a
   * UserActionsModel with a single HBase request using the composite dataset. It
   * performs a get/update/put operation, which is protected by the
   * check_conflict field on UserProfileModel from colliding with another
   * get/update/put operation.
   * 
   * @param firstName
   *          The first name of the user we are updating
   * @param lastName
   *          The last name of the user we are updating
   * @param married
   *          True if this person is married. Otherwise false.
   */
  public void updateUserProfile(String firstName, String lastName,
      boolean married) {
    // Get the timestamp we'll use to set the value of the profile_updated
    // action.
    long ts = System.currentTimeMillis();

    // Construct the key we'll use to fetch the user.
    Key key = new Key.Builder(userProfileActionsDataset)
        .add("firstName", firstName)
        .add("lastName", lastName)
        .build();

    // Get the profile and actions entity from the composite dao.
    UserProfileActionsModel2 profileActionsModel = userProfileActionsDataset.get(key);

    // Updating the married status is hairy since our avro compiler isn't setup
    // to compile setters for fields. We have to construct a clone through the
    // builder.
    UserProfileActionsModel2 updatedProfileActionsModel = UserProfileActionsModel2
        .newBuilder(profileActionsModel)
        .setUserProfileModel(
            UserProfileModel2
                .newBuilder(profileActionsModel.getUserProfileModel())
                .setMarried(married).build()).build();
    // Since maps are mutable, we can update the actions map without having to
    // go through the builder like above.
    updatedProfileActionsModel.getUserActionsModel().getActions()
        .put("profile_updated", Long.toString(ts));

    if (!userProfileActionsDataset.put(profileActionsModel)) {
      // If put returns false, a write conflict occurred where someone else
      // updated the row between the times we did the get and put.
      System.out
          .println("Updating the user profile failed due to a write conflict");
    }
  }

  /**
   * Add an action to the user profile.
   * 
   * This method demonstrates how we can use a keyAsColumn map field (the
   * actions field of the UserActionsModel) to add values to the map without
   * having to do a get/update/put operation. When doing the put, it won't
   * remove columns that exist in the row that aren't in the new map we are
   * putting. It will just add the additional columns we are now putting to the
   * row.
   * 
   * @param firstName
   *          The first name of the user we are updating
   * @param lastName
   *          The last name of the user we are updating
   * @param actionType
   *          A string representing the action type which is the key of the map
   * @param actionValue
   *          A string representing the action value.
   */
  public void addAction(String firstName, String lastName, String actionType,
      String actionValue) {
    // Create a new UserActionsModel, and add a new actions map to it with a
    // single action value. Even if one exists in this row, since it has a lone
    // keyAsColumn field, it won't remove any actions that already exist in the
    // actions column family.
    UserActionsModel2 actionsModel = UserActionsModel2.newBuilder()
        .setFirstName(firstName).setLastName(lastName) // key part
        .setActions(new HashMap<String, String>()).build();
    actionsModel.getActions().put(actionType, actionValue);

    // Perform the put.
    userActionsDataset.put(actionsModel);
  }

  /**
   * The main driver method. Doesn't require any arguments.
   * 
   * @param args
   */
  public static void main(String[] args) throws Exception {
    UserProfileDatasetExample example = new UserProfileDatasetExample();

    // Let's create some user profiles
    example.create("John", "Doe", true);
    example.create("Jane", "Doe", false);
    example.create("Foo", "Bar", false);

    // Now print those user profiles. This doesn't include actions
    example.printUserProfies();

    // Now we'll add some user actions to each user
    example.addAction("Jane", "Doe", "last_login", "2013-07-30 00:00:00");
    example.addAction("Jane", "Doe", "ad_click", "example.com_ad_id");
    example.addAction("Foo", "Bar", "last_login", "2013-07-30 00:00:00");

    // Print the user profiles and actions for the Does. This will include the
    // above actions, as well as a profile_created action set when creating the
    // user profiles.
    example.printUserProfileActionsForLastName("Doe");

    // Update Jane to a married status.
    example.updateUserProfile("Jane", "Doe", true);

    // Reprint the user profiles and actions. Jane should now have married true,
    // as well as a new profile_updated timestamp.
    example.printUserProfileActionsForLastName("Doe");
  }
}
