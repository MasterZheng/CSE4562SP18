package edu.buffalo.www.cse4562.processData;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class processSelect {
    public static void SelectData(HashMap<String,Object> parsedStmt) throws IOException{
        HashMap<String,Object> operation = (HashMap<String,Object>)parsedStmt.get("OPERATIONS");
        //TODO
        String[] headers = {};
        String filePath="/test/R.csv";

        CSVFormat formator = CSVFormat.DEFAULT.withHeader(headers);
        FileReader fileReader=new FileReader(filePath);
        CSVParser parser=new CSVParser(fileReader,formator);
        Iterator<CSVRecord> CSVInterator= parser.iterator();
//      List<CSVRecord> records=parser.getRecords();
        System.out.println(CSVInterator.next());

        parser.close();
        fileReader.close();
    }


}
