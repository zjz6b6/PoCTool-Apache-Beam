package com.infosys.comparisontool;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;

public class CSVHeaders {

    public static String[] convertCSVFileToObject(String file) {
        // list of csv cells will be stored in this list and returned
        ArrayList<String> headers = new ArrayList<>();
        String[] result = null;
        try {
            Reader in = new FileReader(file);

            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);

            for (CSVRecord record : records) {
                for (int x = 0; x < record.size(); x++) {
                    headers.add(record.get(x).replaceAll(" ", ""));
                }
                break;
            }

            result = headers.toArray(new String[headers.size()]);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String getHeaders(String file) {
        try {
            Reader in = new FileReader(file);
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);

            for (CSVRecord record : records) {
                return record.get(0).replaceAll(" ", "");
            }


//            }
        } catch (IOException e ){
            e.printStackTrace();
        }

        return null;
    }
}
