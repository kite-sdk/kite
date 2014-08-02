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
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.slf4j.Logger;

@Parameters(commandDescription = "Create a generic SOLR-schema to index a Kite Dataset")
public class SolrSchemaCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset name>")
  List<String> datasets;
  
  String refFile1 = "/refschema.xml";
  String refFile2 = "/refmorphline.conf";

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
      String datasetName = datasets.get(0); 		
      Schema dsSchema = load(datasetName, Object.class)
        .getDataset()
        .getDescriptor()
        .getSchema();	

      String collection = load(datasetName, Object.class)
    	        .getDataset().getName();
       
  	  System.out.println( dsSchema.toString() );

	  System.out.println( ">>> Start the schema conversion for collection :" + collection + " now ... " );
	  System.out.println( ">     Use the reference-schema    : " + refFile1 );
	  System.out.println( ">     Use the reference-morphline : " + refFile2 );
	  
	  List<Field> listOfFields = dsSchema.getFields();

	  String ex = "\n";
	  String morphFields = "	  ";
	  
	  for( Field fi : listOfFields ) { 
		  String field = fi.name();
		  ex = ex + "	  <field name=\"" + field + "\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\" />\n"; 
		  morphFields = morphFields + field + " : /" + field + "\n	  "; 
	  }
	  ex = ex + "	  <field name=\"_version_\" type=\"long\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>\n" +
		   "	  <dynamicField name=\"ignored_*\" type=\"ignored\"/>\n";

	  File fout = new File("schema.xml");

	  System.out.println( ">     This are your fields : \n" + ex );
	  System.out.println( ">>> Created solr-schema  : " + fout.getAbsolutePath() );
	  
 	  StringBuffer sb = new StringBuffer();
	  BufferedReader br = new BufferedReader( new InputStreamReader( SolrSchemaCommand.class.getResourceAsStream( refFile1 ) ) );
	  while (br.ready()) { 
		  String line = br.readLine();
		  if ( line.contains( "___FIELDS___" ) )
			  line = ex;
		  sb.append(line);
	  }

	  FileWriter fw = new FileWriter(fout);
	  fw.write( sb.toString() );
	  fw.close();

	  System.out.println( ">>> Created solr-schema  : " + fout.getAbsolutePath() );
	  
	  System.out.print( ">   This are your fields : \n" + ex );
	  
	  StringBuffer sb2 = new StringBuffer();
	  BufferedReader br2 = new BufferedReader( new InputStreamReader( SolrSchemaCommand.class.getResourceAsStream( refFile2 ) ) );
	  while (br2.ready()) { 
		  String line = br2.readLine();
		  if ( line.contains( "___FIELDS___" ) )
			  line = morphFields;
		  sb2.append(line);
	  }
	  
	  String ml = sb2.toString().replace("___COLLECTION___", collection );
	  
	  File fout2 = new File( collection + "-csv-morphlines.conf");
	  System.out.println( ">>> Created morphline-file : " + fout2.getAbsolutePath() );
	  FileWriter fw2 = new FileWriter(fout2);
	  fw2.write( sb2.toString() );
	  fw2.close();

	  System.out.println( ">   Fields in morphline : \n" + morphFields );
	  
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
	// TODO Auto-generated method stub
	return new ArrayList<String>();
}

   

}
