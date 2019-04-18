package com.poc.flink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;

import com.poc.flink.model.CSVComparisonEntry;

public class TestFlink {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		CsvReader csvReader = env.readCsvFile("credit_test_data.csv");
		csvReader = csvReader.ignoreFirstLine();
		csvReader = csvReader.fieldDelimiter(",");
		
		DataSource<CSVComparisonEntry> dataSource = csvReader.pojoType(CSVComparisonEntry.class, "csvValue");
		List<CSVComparisonEntry> list = dataSource.collect();
		System.out.println("credit_test_data.csv:");
		list.forEach(System.out::println);
		
		CsvReader csvReader2 = env.readCsvFile("credit_train_data.csv");
		csvReader2 = csvReader2.ignoreFirstLine();
		csvReader2 = csvReader2.fieldDelimiter(",");
		
		DataSource<CSVComparisonEntry> dataSource2 = csvReader2.pojoType(CSVComparisonEntry.class, "csvValue");
		List<CSVComparisonEntry> list2 = dataSource2.collect();
		System.out.println("credit_train_data.csv:");
		list2.forEach(System.out::println);
		
		List<List<CSVComparisonEntry>> data = new ArrayList<>();
		
		data.add(list);
		data.add(list2);
		
		
		for(int i=0;i<data.size();i++) {
			System.out.println(data.get(i));
		}
		
		// env.execute();
		

	}

}
