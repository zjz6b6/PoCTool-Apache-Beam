package com.poc.flink.model;

import java.io.Serializable;

/**
 * Class that represents an entry in the CSV file
 * 
 * @author mark.lupine
 *
 */
public class CSVComparisonEntry implements Serializable {

	//private CSVComparisonCompositeKey csvComparisonCompositeKey;
	//private CSVComparisonMappingType csvComparisonMappingType;
	public String csvValue;
	
	public CSVComparisonEntry() {}
	
	public CSVComparisonEntry(String csvValue) {
		this.csvValue = csvValue;
	}

	/* public CSVComparisonEntry(CSVComparisonCompositeKey csvComparisonCompositeKey,
			CSVComparisonMappingType csvComparisonMappingType, String csvValue) {
		this.csvComparisonCompositeKey = csvComparisonCompositeKey;
		this.csvComparisonMappingType = csvComparisonMappingType;
		this.csvValue = csvValue;
	} */
	
	public String toString() {
		return "The composite key is: and csv Value is "+ csvValue;
	}
	
	public void setCsvValue(String value) {
		this.csvValue = value;
	}
	
	
	/*public CSVComparisonCompositeKey getCSVComparisonCompositeKey() {
		return this.csvComparisonCompositeKey;
	} */
	
	public String getCsvValue() {
		return csvValue;
	}
	
	/* public CSVComparisonMappingType getCsvComparisonMappingType() {
		return csvComparisonMappingType;
	}
	
	public String getCellCoordinates() {
		return csvComparisonCompositeKey.getRow()+ "-"+ csvComparisonCompositeKey.getColumn();
	}
	
	@Override
	public boolean equals(Object object) {
		
		if ((object instanceof CSVComparisonEntry) == false) return false;
		
		CSVComparisonEntry otherCSVComparisonEntry = (CSVComparisonEntry) object;
		
		boolean csvCompKeyMatch = this.csvComparisonCompositeKey.equals(otherCSVComparisonEntry.csvComparisonCompositeKey);
		boolean csvCompMappingType = this.csvComparisonMappingType == otherCSVComparisonEntry.csvComparisonMappingType;
		boolean csvCompValue = this.csvValue.equals(otherCSVComparisonEntry.csvValue);
		
		return (csvCompKeyMatch && csvCompMappingType && csvCompValue);

	}
	
	@Override
	public int hashCode() {
		return this.csvComparisonCompositeKey.hashCode();
	} */
}
