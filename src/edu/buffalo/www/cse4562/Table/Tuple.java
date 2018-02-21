package edu.buffalo.www.cse4562.Table;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.commons.csv.CSVRecord;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Tuple {
    //private TableObject tableObject;
    private HashMap<String, Object> attributes = new HashMap<>();
    private boolean Empty;
    private String  tableName;
    public Tuple() {
        this.Empty = true;
    }

    public Tuple(TableObject tableObject, CSVRecord record) {
        //this.tableObject = tableObject;
        int i = 0;
        for (ColumnDefinition c : tableObject.getColumnDefinitions()) {
            attributes.put(c.getColumnName().toUpperCase(), record.get(i++));
        }
        this.Empty = false;
    }


    //    public TableObject getTableObject() {
//        return tableObject;
//    }
//
//    public void setTableObject(TableObject tableObject) {
//        this.tableObject = tableObject;
//    }

    public HashMap<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(HashMap attributes) {
        this.attributes = attributes;
    }

    public boolean isEmpty() {
        return Empty;
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String table) {
        this.tableName = table;
    }


    public void print() {
        String row = "";

        for (Iterator<Map.Entry<String, Object>> attIter = attributes.entrySet().iterator();attIter.hasNext();){
            row+=attIter.next().getValue();
            if (attIter.hasNext()){
                row+="|";
            }
        }

        System.out.println(row);

    }
}
