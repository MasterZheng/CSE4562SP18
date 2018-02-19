package edu.buffalo.www.cse4562.Table;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.commons.csv.CSVRecord;

import java.util.HashMap;
import java.util.List;

public class Tuple {
    private TableObject tableObject;
    private HashMap<String,String> attributes = new HashMap<>();
    private CSVRecord record = null;

    public Tuple(){

    }
    public Tuple(TableObject tableObject, CSVRecord record) {
        this.tableObject = tableObject;
        this.record = record;
        List<ColumnDefinition> columnDefinitions = tableObject.getColumnDefinitions();
        int i = 0;
        for (ColumnDefinition c:columnDefinitions){
            attributes.put(c.getColumnName().toUpperCase(),record.get(i));
        }

    }

    public TableObject getTableObject() {
        return tableObject;
    }

    public void setTableObject(TableObject tableObject) {
        this.tableObject = tableObject;
    }

    public HashMap<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(HashMap attributes) {
        this.attributes = attributes;
    }
}
