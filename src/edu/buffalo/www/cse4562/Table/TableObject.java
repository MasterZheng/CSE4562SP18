package edu.buffalo.www.cse4562.Table;

import edu.buffalo.www.cse4562.RA.RANode;
import edu.buffalo.www.cse4562.RA.RATable;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.File;
import java.util.*;
import java.util.logging.Logger;

public class TableObject {
    private Table table;
    private String tableName;
    private String fileDir;
    private String alisa;
    private List<ColumnDefinition> columnDefinitions;
    //when the table is a query result, it is necessary to record the table info about the column
    private List<Column> columnInfo= new ArrayList<>();
    private List<Tuple> tupleList = new ArrayList<>();

    static Logger logger = Logger.getLogger(TableObject.class.getName());

    public TableObject(TableObject tableObject, RANode raTable){

        this.table = ((RATable)raTable).getTable();
        this.tableName = ((RATable)raTable).getTable().getName();
        this.alisa = this.table.getAlias();
        this.columnDefinitions = tableObject.getColumnDefinitions();
        this.columnInfo = tableObject.getColumnInfo();
        this.fileDir = tableObject.getFileDir();

    }
    public TableObject(){

    }

    public TableObject(CreateTable createTable, Table table, String tableName) {
        this.table = table;
        this.tableName = tableName;
        this.columnDefinitions = createTable.getColumnDefinitions();
        for (ColumnDefinition c:this.columnDefinitions)
            columnInfo.add(new Column(table,c.getColumnName()));
        if (this.fileDir == null) {
            fileDir = "data/" + createTable.getTable().getName() + ".dat";
        }
    }


    public String getAlisa() {
        return alisa;
    }

    public void setAlisa(String alisa) {
        this.alisa = alisa;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFileDir() {
        return fileDir;
    }

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    public List<Column> getColumnInfo() {
        return columnInfo;
    }

    public void setColumnInfo(List<Column> columnInfo) {
        this.columnInfo = columnInfo;
    }

    public List<Tuple> getTupleList() {
        return tupleList;
    }

    public void settupleList(List<Tuple> tupleList) {
        this.tupleList = tupleList;
    }

    public Iterator getIterator() {
        return this.tupleList.iterator();
    }

    public void print() {
        Iterator<Tuple> iterator = this.getIterator();
        while (iterator.hasNext()) {
            iterator.next().printTuple(this.columnDefinitions,this.columnInfo);
        }
    }
}
