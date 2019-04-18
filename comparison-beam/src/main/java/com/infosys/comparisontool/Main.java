package com.infosys.comparisontool;

import com.infosys.comparisontool.model.CSVComparisonCompositeKey;
import com.infosys.comparisontool.model.CSVComparisonEntry;
import com.infosys.comparisontool.model.CSVComparisonMappingType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main implements Serializable {

    private CSVComparisonCompositeKey key;
    private CSVComparisonEntry entry;

    public void PoC() throws IOException{

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        File file = new File("src/main/resources/input/file1.csv");
        File fileTwo = new File("src/main/resources/input/file2.csv");

        try {

            PCollection<String> readFileOne = p.apply(
                    "ReadFileOne", TextIO.read()
                            .from(file.getPath())
            );

            PCollection<String> readFileTwo = p.apply(
                    "readFileTwo",
                    TextIO.read().from(fileTwo.getPath()));

            ExecutionEnvironment.getExecutionEnvironment();

            PCollection<List<CSVComparisonEntry>> words =
                readFileOne.apply(ParDo.of(new DoFn<String, List<CSVComparisonEntry>>() {
                    private int row = 1;

                    @ProcessElement
                    public void processElement(ProcessContext c) {

                        String headers = CSVHeaders.getHeaders(file.getPath());
                        if (c.element().equals(headers)) {
                            //can't remember
                        }

                        int col = 1;
                        List<CSVComparisonEntry> entryList = new ArrayList<>();

                        String line = c.element();
                        String[] words = line.split(",");
                        for (String word:words) {
                            entryList.add(new CSVComparisonEntry(new CSVComparisonCompositeKey(row, col),
                                            CSVComparisonMappingType.STRING,
                                            word
                                    )
                            );
                            col++;
                        }
                        row++;
                        System.out.println(row + "-" + col);
                        c.output(entryList);

                    }
                }));

            p.run().waitUntilFinish();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        Main main = new Main();

        main.PoC();
    }
}
