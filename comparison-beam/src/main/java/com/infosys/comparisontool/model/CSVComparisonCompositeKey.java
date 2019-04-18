package com.infosys.comparisontool.model;

import java.io.Serializable;

/**
 * Class that encapsulates the cell mappings within a CSV file.
 * 
 * @author mark.lupine
 *
 */
public class CSVComparisonCompositeKey implements Serializable {
	
	private int row;
	private int column;
	
	public CSVComparisonCompositeKey(int row, int column) {
		this.row = row;
		this.column = column;
	}
	
	public int getRow() {
		return row;
	}
	
	public int getColumn() {
		return column;
	}
	
	@Override
	public boolean equals(Object object) {
		
		if ((object instanceof CSVComparisonCompositeKey) == false) return false;
		
		CSVComparisonCompositeKey otherCSVComparisonCompositeKey = (CSVComparisonCompositeKey) object;
		
		String otherRow = Integer.toString(otherCSVComparisonCompositeKey.row);
		String otherColumn = Integer.toString(otherCSVComparisonCompositeKey.column);
		
		String thisRow = Integer.toString(this.row);
		String thisColumn = Integer.toString(this.column);
		
		String otherCell = otherRow + otherColumn;
		String thisCell = thisRow + thisColumn;
		
		return (thisCell.equals(otherCell));
		
	}
	
	@Override
	public int hashCode() {
		
		String thisRow = Integer.toString(this.row);
		String thisColumn = Integer.toString(this.column);
		
		String thisCell = thisRow + thisColumn;
		
		return thisCell.hashCode();
	}
	
	@Override
	public String toString() {
		return ""+ row + ""+ column;
	}

	public int compareTo(CSVComparisonCompositeKey otherCSVComparisonCompositeKey) {
		
		String thisRow = Integer.toString(this.row);
		String thisColumn = Integer.toString(this.column);
		
		String otherRow = Integer.toString(otherCSVComparisonCompositeKey.row);
		String otherColumn = Integer.toString(otherCSVComparisonCompositeKey.column);
		
		String otherCell = otherRow + otherColumn;
		String thisCell = thisRow + thisColumn;
		
		return thisCell.compareTo(otherCell); 
	}

}
