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


import org.apache.tika.config.ServiceLoader;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.mime.MimeTypesFactory;
import org.apache.tika.metadata.Metadata;

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
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.kitesdk.data.DatasetDescriptor;
import org.slf4j.Logger;

@Parameters(commandDescription = "Show Dublin-Core metadata of a Kite Dataset")
public class DublinCoreCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset name>")
  List<String> datasets;
  
  String refFile1 = "/refschema.xml";
  String refFile2 = "/flume/etosha/triplecollector";
  
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"-o", "--output"}, description="Save DC metadata as N-Triples to path.")

  String outputPath = null;

  public DublinCoreCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasets != null && !datasets.isEmpty(),
        "Missing dataset name");
    if (datasets.size() == 1) {
    	
      Metadata md = new Metadata();
      System.out.print( md );	
    	
      String ds = datasets.get(0); 		
      
      
      DatasetDescriptor dsDescr = load(ds, Object.class)
    	        .getDataset()
    	        .getDescriptor();
      
      String dsName = load(ds, Object.class).getDataset().getName();
      
       
  	  System.out.println( ">>> Dataset name: " + dsName.toString() );

  	  
  	  
	  System.out.println( ">     Use the reference-file 1    : " + refFile1 );
	  System.out.println( ">     Use the reference-file 2    : " + refFile2 );

	  Collection<String> props = dsDescr.listProperties();
	  String rdf = "";
	  for(String p : props ) {
		  rdf = rdf + p + " " + dsDescr.getProperty(p) + " .\n";
	  }
  
	  File fout2 = new File( dsName + "-dc.rdf");
	  System.out.println( ">>> Created RDF-file : " + fout2.getAbsolutePath() );
	  FileWriter fw2 = new FileWriter( fout2 );
	  fw2.write( rdf );
	  fw2.close();
	  
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
