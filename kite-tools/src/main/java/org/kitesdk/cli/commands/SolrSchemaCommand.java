/*
 * Copyright 2013 Cloudera Inc.
 *
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
 * limitations under the License.
 */

package org.kitesdk.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.slf4j.Logger;

@Parameters(commandDescription = "Create a generic SOLR-schema to index a Kite Dataset")
public class SolrSchemaCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset name>")
  List<String> datasets;
  
  String refFile = "refschema.xml";

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"-o", "--output"}, description="Save SOLR schema.xml to path")
  String outputPath = null;

  @Parameter(names="--minimize",
      description="Minimize schema file size by eliminating white space")
  boolean minimize=false;

  public SolrSchemaCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasets != null && !datasets.isEmpty(),
        "Missing dataset name");
    if (datasets.size() == 1) {
      Schema dsSchema = load(datasets.get(0), Object.class)
        .getDataset()
        .getDescriptor()
        .getSchema();	
    
      String solr = "to be done";
      File f = new File( refFile );
	  System.out.println( dsSchema.toString() );

	  System.out.println( ">>> Do the schema conversion now ... " );
	  System.out.println( "> Use the reference-schema: " + f.getAbsolutePath() );
	  
	  List<Field> listOfFields = dsSchema.getFields();

	  String ex = "\n";
	  for( Field fi : listOfFields ) { 
		  String field = fi.name();
		  ex = ex + "	  <field name=\"" + field + "\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\" />"; 
	  }
	  ex = ex + "	  <field name=\"_version_\" type=\"long\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>" +
		   "	  <dynamicField name=\"ignored_*\" type=\"ignored\"/>";
	  
	  StringBuffer sb = new StringBuffer();
	  BufferedReader br = new BufferedReader( new FileReader( f ) );
	  while (br.ready()) { 
		  String line = br.readLine();
		  if ( line.contains( "___FIELDS___" ) )
			  line = ex;
		  sb.append(line);
	  }

	  File fout = new File("schema.xml");
	  FileWriter fw = new FileWriter(fout);
	  fw.write( sb.toString() );
	  fw.close();
	  
	  System.out.println( "> Created solr-schema  : " + fout.getAbsolutePath() );
	  
	  System.out.println( "> This are your fields : \n" + ex );
	  
	  System.out.println( "> Done.");
	  
    } else {
//      Preconditions.checkArgument(outputPath == null,
//          "Cannot output multiple schemas to one file");
//      for (String name : datasets) {
//        console.info("Dataset \"{}\" schema: {}", name, load(name, Object.class)
//            .getDataset()
//            .getDescriptor()
//            .getSchema()
//            .toString(!minimize));
//      }
    }
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print the schema for dataset \"users\" to standard out:",
        "users",
        "# Print the schema for a dataset URI to standard out:",
        "dataset:hbase:zk1,zk2/users",
        "# Save the schema for dataset \"users\" to user.avsc:",
        "users -o user.avsc"
    );
  }

}
