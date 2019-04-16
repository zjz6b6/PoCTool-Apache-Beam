package org.apache.beam;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
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
	  
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);



    
    //Read from all files in the files/ directory

	  try{
		  	//Read file from files/ directory
			 CSVReader trainReader = new CSVReader(new FileReader("files/credit_train_data.csv"));
			 CSVReader testReader = new CSVReader(new FileReader("files/credit_test_data.csv"));
			 String[] nextLine;
			 ArrayList<String> csvTrainLines = new ArrayList<>();
			 ArrayList<String> csvTestLines = new ArrayList<>();
			 //Iterate through each line printing its contents			 
			 while((nextLine = trainReader.readNext()) != null){
				 if(nextLine != null){
					 csvTrainLines.add(Arrays.toString(nextLine));
//					 System.out.println(Arrays.toString(nextLine));		  
				 }
			 }
			 
			 while((nextLine = testReader.readNext()) != null){
				 if(nextLine != null){
					 csvTestLines.add(Arrays.toString(nextLine));
//					 System.out.println(Arrays.toString(nextLine));		  
				 }
			 }

			 
			//Split the lines based on ',' appearing [Loan ID Customer ID Loan Status Current Loan Amount Term Credit Score]		 
			String TRAIN_HEADERS_SPLIT[] = csvTrainLines.get(0).split(",");
			ArrayList<String>TRAIN_HEADERS = new ArrayList<>();
			
			String TEST_HEADERS_SPLIT[] = csvTrainLines.get(0).split(",");
			ArrayList<String>TEST_HEADERS = new ArrayList<>();	
			//Iterate adding the HEADERS	
//			for (int i = 0; i < TRAIN_HEADERS_SPLIT.length; i++) {
//				TRAIN_HEADERS.add(TRAIN_HEADERS_SPLIT[i]);
//			}
			//Iterate printing the HEADERS
//			for (int i = 0; i < TRAIN_HEADERS.size(); i++) {
//				System.out.println(TRAIN_HEADERS.get(i));
//			}	
			
				//Iterate printing the second header
//				System.out.println("train_headers: "+TRAIN_HEADERS.get(1));
				
				
				for (int i = 0; i < TEST_HEADERS_SPLIT.length; i++) {
					TEST_HEADERS.add(TEST_HEADERS_SPLIT[i]);
				}
				//Iterate printing the HEADERS
//				for (int i = 0; i < TEST_HEADERS.size(); i++) {
//					System.out.println(TEST_HEADERS.get(i));
//				}	
				
					//Iterate printing the second header
//					System.out.println("test_headers: "+TEST_HEADERS.get(1));
			    	
					p.apply(Create.of(TEST_HEADERS)).setCoder(StringUtf8Coder.of())
					.apply(TextIO.write().to("headers"));
				
					p.run().waitUntilFinish();
			
		}catch(Exception e){
			System.out.println(e);
		}

  
   
  }
}
		  
