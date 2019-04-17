package org.apache.beam.comparisontool;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.comparisontool.model.CSVComparisonCompositeKey;
import org.apache.beam.comparisontool.model.CSVComparisonEntry;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;


public class MinimalWordComparator {
	
	

  static boolean areCellsEqual = false;
  
  public static void main(String[] args) {
	  
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);



    
    //Read from all files in the files/ directory

	  try{
		  	//Read file from files/ directory
//			 CSVReader trainReader = new CSVReader(new FileReader("files/credit_train_data.csv"));
//			 CSVReader testReader = new CSVReader(new FileReader("files/credit_test_data.csv"));
//			 String[] nextLine;
//			 ArrayList<String> csvTrainLines = new ArrayList<>();
//			 ArrayList<String> csvTestLines = new ArrayList<>();
//			 //Iterate through each line printing its contents			 
//			 while((nextLine = trainReader.readNext()) != null){
//				 if(nextLine != null){
//					 csvTrainLines.add(Arrays.toString(nextLine));
//					 System.out.println(Arrays.toString(nextLine));		  
//				 }
//			 }
			 
//			 while((nextLine = testReader.readNext()) != null){
//				 if(nextLine != null){
//					 csvTestLines.add(Arrays.toString(nextLine));
//					 System.out.println(Arrays.toString(nextLine));		  
//				 }
//			 }

			 
			//Split the lines based on ',' appearing [Loan ID Customer ID Loan Status Current Loan Amount Term Credit Score]		 
//			String TRAIN_HEADERS_SPLIT[] = csvTrainLines.get(0).split("[,|[+|]+//]");
//			ArrayList<String>TRAIN_HEADERS = new ArrayList<>();
			
//			String TEST_HEADERS_SPLIT[] = csvTrainLines.get(0).split("[,|[+|]+//]");
//			ArrayList<String>TEST_HEADERS = new ArrayList<>();	
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
				
				
//				for (int i = 0; i < TEST_HEADERS_SPLIT.length; i++) {
//					TEST_HEADERS.add(TEST_HEADERS_SPLIT[i]);
//				}
				//Iterate printing the HEADERS
//				for (int i = 0; i < TEST_HEADERS.size(); i++) {
//					System.out.println(TEST_HEADERS.get(i));
//				}	
				
					//Iterate printing the second header
//					System.out.println("test_headers: "+TEST_HEADERS.get(1));
		  
		  CSVFileLoader fileOneMap = new CSVFileLoader("files/credit_test_data.csv");
		  CSVFileLoader fileTwoMap = new CSVFileLoader("files/credit_train_data.csv");

		  Map<CSVComparisonCompositeKey, CSVComparisonEntry> map = new HashMap<>();
		  map = fileOneMap.call();
		  Map<CSVComparisonCompositeKey, CSVComparisonEntry> map2 = new HashMap<>();
		  map=fileTwoMap.call();
		
		 Map<String,String> mapString = new HashMap();
		
		 for (CSVComparisonCompositeKey key: map.keySet()) {
//			System.out.println(map.get(key).getCsvValue());
			mapString.put(key.getRow() + "-" + key.getColumn(), map.get(key).getCsvValue());
		 }
		 
		 for(String key: mapString.keySet()) {
//			 System.out.println(mapString.get(key));
			 p.apply(Create.of(mapString.get(key))).setCoder(StringUtf8Coder.of()).apply(Filter.by((String cell) -> !cell.isEmpty())).apply(Count.perElement()).apply(
			            MapElements.into(TypeDescriptors.strings())
		                .via(
		                    (KV<String, Long> cellCount) ->
		                        "Cells are equal" + "? " + (areCellsEqual = (cellCount.getValue() > 1)) ));
			 
		 }
		 
		 
		
				
				
			
			

				
		
					
				
//					.apply(TextIO.write().to("cells"));
					
//					p.apply(Create.of(mapString)).setCoder(StringUtf8Coder.of());
//					.apply(TextIO.write().to("headers"));
				
					p.run().waitUntilFinish();
					
					if(areCellsEqual){
				        System.out.println("CELLS ARE EQUAL, CONTINUE WITH PROCESSING");
					//We can probably start a new process here using p.apply() since we know headers are equal
				    }else{
				        System.out.println("CELLS ARE NOT EQUAL, STOPPING");
				    }
			
		}catch(Exception e){
			System.out.println(e);
		}

  
   
  }
}
		  
