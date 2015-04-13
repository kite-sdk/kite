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

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;

import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.hbase.avro.SpecificAvroDao;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.EntityScanner;
import org.kitesdk.data.hbase.impl.SchemaManager;
import org.kitesdk.data.hbase.manager.DefaultSchemaManager;
import org.kitesdk.data.hbase.tool.SchemaTool;

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
 * putting, composite DAOs, and optimistic concurrency control (OCC).
 */
public class UserProfileExample {

  /**
   * The user profile DAO
   */
  private final Dao<UserProfileModel> userProfileDao;

  /**
   * The user actions DAO. User actions are stored in the same table along side
   * the user profiles.
   */
  private final Dao<UserActionsModel> userActionsDao;

  /**
   * A composite dao that encapsulates the two entity types that can be stored
   * in the user_profile table (UserProfileModel and UserActionsModel).
   * UserProfileActionsModel is the compsite type this dao returns.
   */
  private final Dao<UserProfileActionsModel> userProfileActionsDao;

  /**
   * The constructor will start by registering the schemas with the meta store
   * table in HBase, and create the required tables to run.
   */
  public UserProfileExample() throws InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    HTablePool pool = new HTablePool(conf, 10);
    SchemaManager schemaManager = new DefaultSchemaManager(pool);

    registerSchemas(conf, schemaManager);

    userProfileDao = new SpecificAvroDao<UserProfileModel>(pool,
        "kite_example_user_profiles", "UserProfileModel", schemaManager);
    userActionsDao = new SpecificAvroDao<UserActionsModel>(pool,
        "kite_example_user_profiles", "UserActionsModel", schemaManager);
    userProfileActionsDao = SpecificAvroDao.buildCompositeDaoWithEntityManager(
        pool, "kite_example_user_profiles", UserProfileActionsModel.class,
        schemaManager);
  }

  /**
   * Print all user profiles.
   * 
   * This method demonstrates how to open a scanner that will scan the entire
   * table. It has no start or stop keys specified.
   */
  public void printUserProfies() {
    EntityScanner<UserProfileModel> scanner = userProfileDao.getScanner();
    scanner.initialize();
    try {
      for (UserProfileModel entity : scanner) {
        System.out.println(entity.toString());
      }
    } finally {
      // scanners need to be closed.
      scanner.close();
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
    // Create a partial key that will allow us to start the scanner from the
    // first user record that has last name equal to the one provided.
    PartitionKey startKey = new PartitionKey("lastName");

    // Get the scanner with the start key. Null for stopKey in the getScanner
    // method indicates that the scanner will scan to the end of the table. Our
    // loop will break out when it encounters a record without the last name.

    EntityScanner<UserProfileActionsModel> scanner = userProfileActionsDao
        .getScanner(startKey, null);
    scanner.initialize();
    try {
      // scan until we find a last name not equal to the one provided
      for (UserProfileActionsModel entity : scanner) {
        if (!entity.getUserProfileModel().getLastName().equals(lastName)) {
          // last name of row different, break out of the scan.
          break;
        }
        System.out.println(entity.toString());
      }
    } finally {
      // scanners need to be closed.
      scanner.close();
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

    UserProfileModel profileModel = UserProfileModel.newBuilder()
        .setFirstName(firstName).setLastName(lastName).setMarried(married)
        .setCreated(ts).build();
    UserActionsModel actionsModel = UserActionsModel.newBuilder()
        .setFirstName(firstName).setLastName(lastName)
        .setActions(new HashMap<String, String>()).build();
    actionsModel.getActions().put("profile_created", Long.toString(ts));

    UserProfileActionsModel profileActionsModel = UserProfileActionsModel
        .newBuilder().setUserProfileModel(profileModel)
        .setUserActionsModel(actionsModel).build();

    if (!userProfileActionsDao.put(profileActionsModel)) {
      // If put returns false, a user already existed at this row
      System.out
          .println("Creating a new user profile failed due to a write conflict.");
    }
  }

  /**
   * Update the married status of a new user record.
   * 
   * This method demonstrates updating both a UserProfileModel and a
   * UserActionsModel with a single HBase request using the composite dao. It
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
    PartitionKey key = new PartitionKey(lastName, firstName);

    // Get the profile and actions entity from the composite dao.
    UserProfileActionsModel profileActionsModel = userProfileActionsDao
        .get(key);

    // Updating the married status is hairy since our avro compiler isn't setup
    // to compile setters for fields. We have to construct a clone through the
    // builder.
    UserProfileActionsModel updatedProfileActionsModel = UserProfileActionsModel
        .newBuilder(profileActionsModel)
        .setUserProfileModel(
            UserProfileModel
                .newBuilder(profileActionsModel.getUserProfileModel())
                .setMarried(married).build()).build();
    // Since maps are mutable, we can update the actions map without having to
    // go through the builder like above.
    updatedProfileActionsModel.getUserActionsModel().getActions()
        .put("profile_updated", Long.toString(ts));

    if (!userProfileActionsDao.put(updatedProfileActionsModel)) {
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
    UserActionsModel actionsModel = UserActionsModel.newBuilder()
        .setLastName(lastName).setFirstName(firstName)
        .setActions(new HashMap<String, String>()).build();
    actionsModel.getActions().put(actionType, actionValue);

    // Perform the put.
    userActionsDao.put(actionsModel);
  }

  /**
   * Uses SchemaTool to register the required schemas and create the required
   * tables.
   * 
   * @param conf
   *          The HBaseConfiguration.
   * @param schemaManager
   *          The schema manager SchemaTool needs to create the schemas.
   */
  private void registerSchemas(Configuration conf, SchemaManager schemaManager)
      throws InterruptedException {
    HBaseAdmin admin;
    try {
      // Construct an HBaseAdmin object (required by schema tool), and delete it
      // if it exists so we start fresh.
      admin = new HBaseAdmin(conf);
      if (admin.tableExists("kite_example_user_profiles")) {
        admin.disableTable("kite_example_user_profiles");
        admin.deleteTable("kite_example_user_profiles");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // Use the SchemaTool to create the schemas that are in the example-models
    // directory, and create the table and column families required by those
    // schemas.
    SchemaTool tool = new SchemaTool(admin, schemaManager);
    tool.createOrMigrateSchemaDirectory("classpath:example-models", true);
  }

  /**
   * The main driver method. Doesn't require any arguments.
   * 
   * @param args
   */
  public static void main(String[] args) throws InterruptedException {
    UserProfileExample example = new UserProfileExample();

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
