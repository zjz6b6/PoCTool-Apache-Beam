package org.apache.beam;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.opencsv.CSVReader;

import org.apache.beam.sdk.values.PCollection;


public class MinimalWordComparator {
	
	

//  static boolean areHeadersEqual = false;
  
  public static void main(String[] args) {
	  
//    PipelineOptions options = PipelineOptionsFactory.create();
//    Pipeline p = Pipeline.create(options);


//    p.run().waitUntilFinish();
    
    //Read from all files in the files/ directory

	  try{
		  	//Read file from files/ directory
			 CSVReader reader = new CSVReader(new FileReader("files/credit_train_data.csv"));
			 String[] nextLine;
			 ArrayList<String> rows = new ArrayList<>();
			 //Iterate through each line printing its contents			 
			 while((nextLine = reader.readNext()) != null){
				 if(nextLine != null){
					 rows.add(Arrays.toString(nextLine));
					 System.out.println(Arrays.toString(nextLine));		  
				 }
			 }

			 
			//Split the lines based on ',' appearing [Loan ID Customer ID Loan Status Current Loan Amount Term Credit Score]		 
			String HEADERS_SPLIT[] = rows.get(0).split(",");
			ArrayList<String>HEADERS = new ArrayList<>();		
			//Iterate adding the HEADERS	
			for (int i = 0; i < HEADERS_SPLIT.length; i++) {
				HEADERS.add(HEADERS_SPLIT[i]);
			}
			//Iterate printing the HEADERS
			for (int i = 0; i < HEADERS.size(); i++) {
				System.out.println(HEADERS.get(i));
			}	
			
				//Iterate printing the second header
				System.out.println(HEADERS.get(1));
			
		}catch(Exception e){
			System.out.println(e);
		}

  
   
  }
}
		  
