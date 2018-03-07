package edu.buffalo.www.cse4562.Table;


import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.commons.csv.CSVRecord;

import java.util.*;

public class Tuple {
    private HashMap<String, PrimitiveValue> attributes = new HashMap<>();
    private boolean Empty;
    private String tableName;

    public Tuple() {
        this.Empty = true;
    }

    public Tuple(TableObject tableObject, CSVRecord record) {
        int i = 0;
        for (ColumnDefinition c : tableObject.getColumnDefinitions()) {
            if (c.getColDataType().toString().toUpperCase().equals("INT") || c.getColDataType().toString().toUpperCase().equals("LONG")) {
                attributes.put(c.getColumnName().toUpperCase(), new LongValue(record.get(i++)));
            } else if (c.getColDataType().toString().toUpperCase().equals("STRING")) {
                attributes.put(c.getColumnName().toUpperCase(), new StringValue(record.get(i++)));
            } else if (c.getColDataType().toString().toUpperCase().equals("DATE")) {
                attributes.put(c.getColumnName().toUpperCase(), new DateValue(record.get(i++)));
            } else {
                attributes.put(c.getColumnName().toUpperCase(), new NullValue());
            }
        }

        this.Empty = false;
        this.tableName = tableObject.getTableName();
    }

    public HashMap<String, PrimitiveValue> getAttributes() {
        return attributes;
    }

    public void setAttributes(HashMap<String, PrimitiveValue> attributes) {
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

    public void printTuple(List<ColumnDefinition> list) {
        String row = "";

        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getColDataType().getDataType().equals("STRING")) {
                row += attributes.get(list.get(i).getColumnName());
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



