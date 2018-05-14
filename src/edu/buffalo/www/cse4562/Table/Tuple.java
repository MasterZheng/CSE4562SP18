package edu.buffalo.www.cse4562.Table;


import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.commons.csv.CSVRecord;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;

public class Tuple implements Serializable{
    static  Logger logger = Logger.getLogger(Tuple.class.getName());

    private HashMap< Column, PrimitiveValue> attributes = new HashMap<>();
    private ArrayList<String> tableName = new ArrayList<>();

    public Tuple() {

    }

    public Tuple(TableObject tableObject, CSVRecord record) {
        int i = 0;
        Table table = new Table();
        if (tableObject.getAlisa() != null) {
            table.setName(tableObject.getAlisa());
            this.setTableName(tableObject.getAlisa());
        } else {
            table.setName(tableObject.getTableName());
        }
        this.tableName.add(tableObject.getTableName());

        List<Integer> mapRelations = tableObject.getMapRelations();
        if (0==mapRelations.size()){
            for (int j = 0;j<tableObject.getColumnDefinitions().size();j++) {
                ColumnDefinition c = tableObject.getColumnDefinitions().get(j);
                if (c.getColDataType().toString().toUpperCase().equals("INT") || c.getColDataType().toString().toUpperCase().equals("INTEGER") || c.getColDataType().toString().toUpperCase().equals("LONG")) {
                    attributes.put(new Column(table, c.getColumnName().toUpperCase()), new LongValue(record.get(i++)));
                } else if (c.getColDataType().toString().toUpperCase().equals("STRING")) {
                    attributes.put(new Column(table, c.getColumnName().toUpperCase()), new StringValue(record.get(i++)));
                } else if (c.getColDataType().toString().toUpperCase().equals("DOUBLE")) {
                    attributes.put(new Column(table, c.getColumnName().toUpperCase()), new DoubleValue(record.get(i++)));
                } else if (c.getColDataType().toString().toUpperCase().equals("DATE")) {
                    attributes.put(new Column(table, c.getColumnName().toUpperCase()), new DateValue(record.get(i++)));
                } else {
                    attributes.put(new Column(table, c.getColumnName().toUpperCase()), new NullValue());
                }
            }
        }else {
            for (int j = 0;j<mapRelations.size();j++){
                ColumnDefinition c = tableObject.getColumnDefinitions().get(j);
                if (c.getColDataType().toString().toUpperCase().equals("INT") || c.getColDataType().toString().toUpperCase().equals("INTEGER") || c.getColDataType().toString().toUpperCase().equals("LONG")) {
                    attributes.put(new Column(table, c.getColumnName().toUpperCase()), new LongValue(record.get(mapRelations.get(j))));
                } else if (c.getColDataType().toString().toUpperCase().equals("STRING")) {
                    attributes.put(new Column(table, c.getColumnName().toUpperCase()), new StringValue(record.get(mapRelations.get(j))));
                } else if (c.getColDataType().toString().toUpperCase().equals("DOUBLE")) {
                    attributes.put(new Column(table, c.getColumnName().toUpperCase()), new DoubleValue(record.get(mapRelations.get(j))));
                } else if (c.getColDataType().toString().toUpperCase().equals("DATE")) {
                    attributes.put(new Column(table, c.getColumnName().toUpperCase()), new DateValue(record.get(mapRelations.get(j))));
                } else {
                    attributes.put(new Column(table, c.getColumnName().toUpperCase()), new NullValue());
                }
            }
        }

    }

    public Tuple joinTuple(Tuple right) {
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

    public void printTuple(List<ColumnDefinition> colDef, List<Column> colInfo) {
        String row = "";

        for (int i = 0; i < colInfo.size(); i++) {
            row += attributes.get(colInfo.get(i));
            if (colInfo.size() != 1 && i < colInfo.size() - 1) {
                row += "|";
            }
        }
        if (false) {
            logger.info(row);
        }
        System.out.println(row);
    }

}



