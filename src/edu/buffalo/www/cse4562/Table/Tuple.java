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
    private List<ColumnDefinition> columnDefinitions;
    private boolean Empty;
    private String tableName;

    public Tuple() {
        this.Empty = true;
    }

    public Tuple(TableObject tableObject, CSVRecord record) {
        int i = 0;
        this.columnDefinitions = tableObject.getColumnDefinitions();
        for (ColumnDefinition c : this.columnDefinitions) {
            attributes.put(c.getColumnName().toUpperCase(), record.get(i++));
        }
        this.Empty = false;
        this.tableName = tableObject.getTableName();
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

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    public void print() {
        String row = "";
        List<ColumnDefinition> list = this.getColumnDefinitions();
//        for (Iterator<Map.Entry<String, Object>> attIter = attributes.entrySet().iterator();attIter.hasNext();){
//            Map.Entry<String, Object> column = attIter.next();
        for (int i = 0; i < list.size();i++) {
            if (list.get(i).getColDataType().getDataType().equals("STRING")) {
                row += "\'" + attributes.get(list.get(i).getColumnName()) + "\'";
            } else {
                row += attributes.get(list.get(i).getColumnName());
            }
            if (list.size() != 1 && i < list.size() - 1) {
                row += "|";
            }
        }
        System.out.println(row);
    }

}



