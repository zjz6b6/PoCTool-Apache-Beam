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
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;


public class MinimalWordComparator {
	
	

  static boolean areLinesEqual = false;
  
  public static void main(String[] args) {
	  
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    //Read from all files in the files/ directory

	  try{
		  
		p.apply(TextIO.read().from("files/*"))
		  .apply(Filter.by((String lines) -> !lines.isEmpty()))
		  .apply(Count.perElement())
		  .apply(
			             MapElements.into(TypeDescriptors.strings())
			                 .via(
			                     (KV<String, Long> lineCount) ->
			                      lineCount.getKey() + ": " + lineCount.getValue()))
		  .apply(TextIO.write().to("data"));
		
		
		  
		  
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
			 p.apply(Create.of(mapString.get(key))).setCoder(StringUtf8Coder.of())
			 .apply(Filter.by((String cell) -> !cell.isEmpty()));
//			 .apply(TextIO.write().to("cells"));
		 }
		 
		 
				
//					.apply(TextIO.write().to("cells"));
					
//					p.apply(Create.of(mapString)).setCoder(StringUtf8Coder.of());
//					.apply(TextIO.write().to("headers"));
				
					p.run().waitUntilFinish();
					
//					if(areLinesEqual){
//				        System.out.println("LINES ARE EQUAL, CONTINUE WITH PROCESSING");
//				      
//					//We can probably start a new process here using p.apply() since we know headers are equal
//				    }else{
//				        System.out.println("LINES ARE NOT EQUAL, STOPPING");
//				    }
			
		}catch(Exception e){
			System.out.println(e);
		}

  
   
  }
}
		  
