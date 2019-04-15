package org.apache.beam;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;


public class MinimalWordComparator {

  static boolean areHeadersEqual = false;

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    //Read from all files in the files/ directory
    p.apply(TextIO.read().from("files/*"))
	//Filter by only lines that aren't empty and do not contain "-".
	//Assuming header lines will never contain "-", and non header lines will always contain "-".
        .apply(Filter.by((String line) -> !line.isEmpty() && !line.contains("-")))
        //Count the frequency of the header lines.
        .apply(Count.perElement())
        //Transform the result into a printable String, if we counted more than one of the same header lines then we know the headers are equal.
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> lineCount) ->
                        "Headers are equal" + "? " + (areHeadersEqual = (lineCount.getValue() > 1)) ));
        
        // Apply a write transform, TextIO.Write, at the end of the pipeline.
        // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
        // formatted strings) to a series of text files.
        //
        // By default, it will write to a set of files with names like headerequality-00001-of-00005
//        .apply(TextIO.write().to("headerequality"));

    p.run().waitUntilFinish();
    
    if(areHeadersEqual){
        System.out.println("HEADERS ARE EQUAL, CONTINUE WITH PROCESSING");
	//We can probably start a new process here using p.apply() since we know headers are equal
    }else{
        System.out.println("HEADERS ARE NOT EQUAL, STOPPING");
    }
  }
}
