package edu.buffalo.www.cse4562.Table;

import com.sun.tools.javac.util.Name;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.commons.csv.CSVRecord;

import java.util.HashMap;
import java.util.List;

public class Tuple {
    //private TableObject tableObject;
    private List<ColumnDefinition> columnDefinitions;
    private HashMap<String, Object> attributes = new HashMap<>();
    private boolean Empty;
    private String  tableName;
    public Tuple() {
        this.Empty = true;
    }

    public Tuple(TableObject tableObject, CSVRecord record) {
        //this.tableObject = tableObject;
        this.columnDefinitions = tableObject.getColumnDefinitions();
        int i = 0;
        for (ColumnDefinition c : columnDefinitions) {
            attributes.put(c.getColumnName().toUpperCase(), record.get(i++));
        }
        this.Empty = false;
    }

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
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
        for (int i = 0; i < columnDefinitions.size(); i++) {
            String colName = columnDefinitions.get(i).getColumnName();
            row += attributes.get(colName);
            if (columnDefinitions.size() == 1 || i == columnDefinitions.size() - 1) {
                break;
            }
            row += "|";
        }
        System.out.println(row);

    }
}
