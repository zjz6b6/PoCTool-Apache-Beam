package com.infosys.comparisontool.model;

import java.io.Serializable;

/**
 * Class that represents an entry in the CSV file
 * 
 * @author mark.lupine
 *
 */
public class CSVComparisonEntry implements Serializable {

	private CSVComparisonCompositeKey csvComparisonCompositeKey;
	private CSVComparisonMappingType csvComparisonMappingType;
	private String csvValue;

	public CSVComparisonEntry(CSVComparisonCompositeKey csvComparisonCompositeKey,
			CSVComparisonMappingType csvComparisonMappingType, String csvValue) {
		this.csvComparisonCompositeKey = csvComparisonCompositeKey;
		this.csvComparisonMappingType = csvComparisonMappingType;
		this.csvValue = csvValue;
	}
	
	public String toString() {
		return "The composite key is: "+ csvComparisonCompositeKey.getRow()+ "-"+
				csvComparisonCompositeKey.getColumn()+ " and csv Value is "+ csvValue;
	}
	
	
	public CSVComparisonCompositeKey getCSVComparisonCompositeKey() {
		return this.csvComparisonCompositeKey;
	}
	
	public String getCsvValue() {
		return csvValue;
	}
	
	public CSVComparisonMappingType getCsvComparisonMappingType() {
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
	}
}
