package org.apache.beam.comparisontool;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.beam.comparisontool.model.CSVComparisonCompositeKey;
import org.apache.beam.comparisontool.model.CSVComparisonEntry;
import org.apache.beam.comparisontool.model.CSVComparisonMappingType;

/**
 * CSV File Loader class that just loads the file 
 * 
 * @author mark.lupine
 *
 */
public class CSVFileLoader implements Callable<Map<CSVComparisonCompositeKey, CSVComparisonEntry>> {

	// Setup the member variables
	
	// The file name of the file to load
	private String filename;

	/**
	 * Constructor for the CSVFileLoader class
	 * @param filename the name of the file
	 */
	public CSVFileLoader(String filename) {
		this.filename = filename;
	}

	/**
	 * Convert the CSV file to a List of comparison entries
	 * @return a list of CSV comparison entries
	 */
	private Map<CSVComparisonCompositeKey, CSVComparisonEntry> convertCSVFileToComparisonEntries() {
		// list of csv cells will be stored in this list and returned
		Map<CSVComparisonCompositeKey, CSVComparisonEntry> csvComparisonEntries = new HashMap<>();
		try (CSVParser csvParser = new CSVParser(new FileReader(filename),
				CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {
			// loop through records in csv file
			for (CSVRecord csvRecord : csvParser) {
				// iterate through the width of the header row
				for (int i = 0; i < csvParser.getHeaderMap().size(); i++) {
					
					CSVComparisonCompositeKey key = new CSVComparisonCompositeKey((int) csvRecord.getRecordNumber() - 1, i);
					
					csvComparisonEntries.put(key, new CSVComparisonEntry(key,
							// Set to STRING for now but switch statement needed to
							// change as needed for the data type
							CSVComparisonMappingType.STRING, csvRecord.get(i)));
				}
			}

		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		return csvComparisonEntries;
	}

	@Override
	public Map<CSVComparisonCompositeKey, CSVComparisonEntry> call() throws Exception {
		return convertCSVFileToComparisonEntries();
	}
}
