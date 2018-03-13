package edu.buffalo.www.cse4562.Table;


import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.commons.csv.CSVRecord;

import java.util.*;

public class Tuple {
    private HashMap<Column, PrimitiveValue> attributes = new HashMap<>();
    private ArrayList<String> tableName = new ArrayList<>();

    public Tuple(){
        
    }
    public Tuple(TableObject tableObject, CSVRecord record) {
        int i = 0;
        for (ColumnDefinition c : tableObject.getColumnDefinitions()) {
            if (c.getColDataType().toString().toUpperCase().equals("INT") ||c.getColDataType().toString().toUpperCase().equals("INTEGER")|| c.getColDataType().toString().toUpperCase().equals("LONG")) {
                attributes.put(new Column(tableObject.getTable(),c.getColumnName().toUpperCase()), new LongValue(record.get(i++)));
            } else if (c.getColDataType().toString().toUpperCase().equals("STRING")) {
                attributes.put(new Column(tableObject.getTable(),c.getColumnName().toUpperCase()), new StringValue(record.get(i++)));
            } else if (c.getColDataType().toString().toUpperCase().equals("DOUBLE")) {
                attributes.put(new Column(tableObject.getTable(),c.getColumnName().toUpperCase()), new DoubleValue(record.get(i++)));
            } else if (c.getColDataType().toString().toUpperCase().equals("DATE")) {
                attributes.put(new Column(tableObject.getTable(),c.getColumnName().toUpperCase()), new DateValue(record.get(i++)));
            } else {
                attributes.put(new Column(tableObject.getTable(),c.getColumnName().toUpperCase()), new NullValue());
            }
        }

        this.tableName.add(tableObject.getTableName());
    }

    public Tuple joinTuple(Tuple right){
        Tuple newTuple = new Tuple();
        HashMap<Column, PrimitiveValue> attributes = new HashMap<>();
        attributes.putAll(this.getAttributes());
        attributes.putAll(right.getAttributes());
        newTuple.setTableName(this.getTableName());
        newTuple.setTableName(right.getTableName());
        newTuple.setAttributes(attributes);
        return newTuple;
    }

    public HashMap<Column, PrimitiveValue> getAttributes() {
        return attributes;
    }

    public void setAttributes(HashMap<Column, PrimitiveValue> attributes) {
        this.attributes = attributes;
    }


    public ArrayList<String> getTableName() {
        return tableName;
    }

    public void setTableName(ArrayList<String> tableName) {
        this.tableName.addAll(tableName);
    }

    public void setTableName(String tableName) {
        this.tableName.add(tableName);
    }
    public void printTuple(List<ColumnDefinition> colDef,List<Column> colInfo) {
        String row = "";

        for (int i = 0; i < colDef.size(); i++) {
            if (colDef.get(i).getColDataType().getDataType().equals("STRING")) {
                row += attributes.get(colInfo.get(i));
            } else {
                row += attributes.get(colInfo.get(i));
            }
            if (colDef.size() != 1 && i < colDef.size() - 1) {
                row += "|";
            }
        }
        System.out.println(row);
    }

}



