package com.infosys.comparisontool;

import com.infosys.comparisontool.model.CSVComparisonCompositeKey;
import com.infosys.comparisontool.model.CSVComparisonEntry;
import com.infosys.comparisontool.model.CSVComparisonMappingType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConvertToEntry extends DoFn<KV<CSVComparisonCompositeKey, CSVComparisonEntry>,
        List<Map<CSVComparisonCompositeKey,CSVComparisonEntry>>> {

    private int row = 1;
    private Map<CSVComparisonCompositeKey, CSVComparisonEntry> entryMap = new HashMap<>();
    private CSVComparisonEntry entry;
    private CSVComparisonCompositeKey key;

    @ProcessElement
    public void processElement(String line) {
        int col = 1;
        String[] words = line.split(",");

        for (String word : words) {
            entry = new CSVComparisonEntry(key = new CSVComparisonCompositeKey(row,col), CSVComparisonMappingType.STRING, word);
            entryMap.put(key, entry);
        }
    }
}
