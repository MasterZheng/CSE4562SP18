package edu.buffalo.www.cse4562.Table;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.commons.csv.CSVRecord;

import java.util.HashMap;
import java.util.List;

public class Tuple {
    private TableObject tableObject;
    private HashMap<String,Object> attributes = new HashMap<>();
    private CSVRecord record = null;
    private boolean Empty;
    public Tuple(){
        this.Empty = true;
    }
    public Tuple(TableObject tableObject, CSVRecord record) {
        this.tableObject = tableObject;
        this.record = record;
        List<ColumnDefinition> columnDefinitions = tableObject.getColumnDefinitions();
        int i = 0;
        for (ColumnDefinition c:columnDefinitions){
            attributes.put(c.getColumnName().toUpperCase(),record.get(i++));
        }
        this.Empty = false;
    }

    public TableObject getTableObject() {
        return tableObject;
    }

    public void setTableObject(TableObject tableObject) {
        this.tableObject = tableObject;
    }

    public HashMap<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(HashMap attributes) {
        this.attributes = attributes;
    }

    public boolean isEmpty() {
        return Empty;
    }
}
